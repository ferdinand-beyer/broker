(ns com.fbeyer.broker
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [com.fbeyer.broker.impl.registry :as registry]))

(defn- drop-all [rf]
  (fn
    ([] (rf))
    ([result] (rf result))
    ([result _] result)))

(defn- null-chan []
  (async/chan (async/sliding-buffer 1) drop-all))

;; ? Use the term 'loop'?
(defmulti ^:private -make-exec
  (fn [broker _ch _f opts]
    (:exec opts (::exec broker))))

;; Run sequentially in a separate thread.
(defmethod -make-exec :sequential [_ ch f _]
  (async/go-loop []
    (when-let [msg (async/<! ch)]
      (async/<! (async/thread (f msg)))
      (recur))))

;; Run sequentially in a go block.
(defmethod -make-exec :go [_ ch f _]
  (async/go-loop []
    (when-let [msg (async/<! ch)]
      (f msg)
      (recur))))

;; Run concurrently in separate threads.
(defmethod -make-exec :concurrent [_ ch f _]
  (async/go-loop [cs [ch]]
    (when (seq cs)
      (let [[v c] (async/alts! cs)]
        (if (and (= c ch) (some? v))
          (recur (conj cs (async/thread (f v))))
          (recur (filterv #(not= % c) cs)))))))

;; Run in a core.async pipeline.
(defmethod -make-exec :pipeline [_ ch f {:keys [parallel]
                                         :or {parallel 1}}]
  (async/pipeline parallel (null-chan) (keep f) ch))

;; Run in a core.async blocking pipeline.
(defmethod -make-exec :blocking [_ ch f {:keys [parallel]
                                         :or {parallel 1}}]
  (async/pipeline-blocking parallel (null-chan) (keep f) ch))

;; Run in a core.async async pipeline.
(defmethod -make-exec :async [_ ch f {:keys [parallel]
                                      :or {parallel 1}}]
  (let [af (fn [msg out]
             (f msg #(async/close! out)))]
    (async/pipeline-async parallel (null-chan) af ch)))

(defn- wrap-error-fn [{::keys [error-fn] :as broker} f]
  (fn [msg]
    (try
      (f msg)
      (catch Throwable e
        (error-fn e {:broker broker
                     :fn f
                     :msg msg})))))

(defn- make-exec [broker ch f opts]
  (-make-exec broker ch (wrap-error-fn broker f) opts))

(defn- make-chan [{::keys [buf-fn]} {:keys [chan]}]
  (or chan (async/chan (buf-fn))))

(defn- write-port? [x]
  (satisfies? async-protocols/WritePort x))

(defn- make-sub [broker recv opts]
  (if (write-port? recv)
    {:ch recv}
    (let [ch (make-chan broker opts)]
      {:ch ch
       :exec-ch (make-exec broker ch recv opts)})))

(defn- run-dispatch [dispatch-ch topic-fn registry]
  (let [todo-cnt  (atom nil)
        done      (async/chan 1)
        delivered (fn [_] (when (zero? (swap! todo-cnt dec))
                            (async/put! done true)))]
    (async/go-loop []
      (if-some [msg (async/<! dispatch-ch)]
        (let [topic (topic-fn msg)
              chs   (registry/topics->chans @registry [::all topic])]
          (when (seq chs)
            (reset! todo-cnt (count chs))
            (doseq [ch chs]
              (when-not (async/put! ch msg delivered)
                (swap! registry registry/remove-sub ch false)))
            (async/<! done))
          (recur))
        (doseq [{:keys [ch]} (registry/all-subs @registry)]
          (async/close! ch))))))

(defn- close-removed [registry ch exec-ch]
  (async/go-loop []
    (if-some [_ (async/<! exec-ch)]
      (recur)
      (swap! registry registry/clear-removed ch)))
  (async/close! ch))

(defn- run-cleanup
  "Watches `registry` and closes removed subscriptions."
  [registry]
  (add-watch registry ::cleanup
             (fn [_ _ old-reg new-reg]
               (let [old (registry/removed-map old-reg)
                     new (registry/removed-map new-reg)]
                 (when (not= old new)
                   (doseq [[ch {:keys [exec-ch]}] new
                           :when (nil? (old ch))]
                     (close-removed registry ch exec-ch)))))))

(defn- thread-uncaught-exc-handler
  [e _]
  (let [thread (Thread/currentThread)]
    (-> (.getUncaughtExceptionHandler thread)
        (.uncaughtException thread e)))
  nil)

(defn start
  "Starts a broker.

   Supported options:
   * `:topic-fn` - function used to determine the topic of an incoming message
     for [[subscribe]].  Defaults to `first`, expecting messages to be vectors
     `[topic payload...]`.
   * `:xform` - a transducer to transform/filter messages as they are published.
     Should not throw exceptions and must not produce `nil` messages.
   * `:buf-or-n` - `core.async` buffer to use for the publish channel.  Defaults
     to a large fixed-size buffer (`1024`).
   * `:buf-fn` - function to create `core.async` buffers for subscribing
     functions.  By default, uses small fixed-size buffers (`8`).
   * `:error-fn` - when a subscribing function throws an exception, this function
     will be called with two arguments: the exception and a map with keys
     `:broker`, `:fn`, and `:msg`.  With no `:error-fn` (default), exceptions are
     passed to the current thread's `UncaughtExceptionHandler`.
   * `:exec` - default subscription execution type -- see [[subscribe]]."
  ([] (start nil))
  ([opts]
   (let [registry    (atom registry/empty-registry)
         dispatch-ch (async/chan (:buf-or-n opts 1024)
                                 (:xform opts))
         loop-ch     (run-dispatch dispatch-ch (:topic-fn opts first) registry)
         _           (run-cleanup registry)]
     {::registry    registry
      ::dispatch-ch dispatch-ch
      ::loop-ch     loop-ch
      ::buf-fn      (:buf-fn opts (constantly 8))
      ::error-fn    (:error-fn opts thread-uncaught-exc-handler)
      ::exec        (:exec opts :sequential)})))

(defn- loop-chans [registry]
  (->> (concat (registry/all-subs registry)
               (registry/removed-subs registry))
       (keep :exec-ch)))

(defn stop-chan
  "Returns a channel that will close when the broker stops and all
   pending messages have been processed by the subscribers.  Can be used
   to block for a graceful shutdown."
  [broker]
  (let [{::keys [loop-ch registry]} broker]
    (async/go
      (async/<! loop-ch)
      (let [ch (async/merge (loop-chans @registry))]
        (loop []
          (when (async/<! ch)
            (recur)))))))

(defn stop!
  "Stops the broker, closing all internal async channels.
   After that, the broker will no longer accept messages.  Any priorly
   published messages will still be delivered.  Can be called multiple
   times.  Returns `nil`."
  [broker]
  (async/close! (::dispatch-ch broker)))

(defn shutdown!
  "Stops the broker and waits for all messages to be processed, or `timeout-ms`
   to pass.  Returns `true` when successfully stopped, or `false` on timeout."
  ([broker]
   (stop! broker)
   (async/<!! (stop-chan broker))
   true)
  ([broker timeout-ms]
   (stop! broker)
   (async/alt!!
     (stop-chan broker) true
     (async/timeout timeout-ms) false)))

(defn publish!
  "Publishes `msg` to subscribers, who will be notified asynchronously.
   `msg` must not be `nil`.  Returns `true` unless `broker` is stopped.

   Under high load, this will block the caller to respect back-pressure.
   As such, this should not be called from `(go ...)` blocks.
   When blocking is not desired, use `core.async/put!` on the channel
   returned by [[publish-chan]], or install a windowed buffer using the
   `:buf-or-n` option of [[start]] to drop messages under high load."
  [broker msg]
  (async/>!! (::dispatch-ch broker) msg))

(defn publish-chan
  "Returns the channel used for publishing messages to the broker.

   Intended for publishing from a `(go ...)` block or advanced usage
   such as bulk publishing / piping into the broker.

   Closing the returned channel will stop the broker."
  [broker]
  (::dispatch-ch broker))

(defn- topics->set [topics]
  (cond
    (nil? topics)        #{::all}
    (sequential? topics) (set topics)
    :else                #{topics}))

(defn subscribe
  "Subscribes to messages from the broker.

   When a `topic` is given, subscribes only to messages of this topic.
   `topic` can also be a sequence of multiple topics to subscribe to.
   Without `topic`, subscribes to all messages.

   If `f` is a channel, it is the caller's responsibility to read messages
   asynchronously, and making sure that the JVM process waits for the consumer
   process to finish.

   Options, only when `f` is a function:
   * `:exec` - subscription execution type:
     * `:sequential` (default) - call `f` in a separate thread, one message at
       a time
     * `:go` - call `f` in a `(go ...)` block, one message at a time
     * `:pipeline`, `:blocking` - call `f` in a `core.async` pipeline, with
       parallelism according to the `:parallel` option
     * `:async` - call `f` in an async pipeline.  `f` is expected to start an
       asynchronous process and return immediately.  It must accept a second
       argument: an unary function to signal the event has been handled.
   * `:parallel` - how many parallel calls to allow when `:type` is a pipeline
      (default: `1`)
   * `:chan` - a custom channel to use for the subscription.  Useful to fine
     tune the buffer or to use a transducer on the channel."
  {:arglists '([broker f]
               [broker f opts]
               [broker topic f]
               [broker topic f opts])}
  ([broker f] (subscribe broker nil f))
  ([broker topic-or-f f-or-opts]
   (if (or (map? f-or-opts) (nil? f-or-opts))
     (subscribe broker nil topic-or-f f-or-opts)
     (subscribe broker topic-or-f f-or-opts nil)))
  ([broker topic f opts]
   (let [registry  (::registry broker)
         sub       (delay (make-sub broker f opts))
         reg-after (swap! registry registry/subscribe f opts (topics->set topic)
                          (partial force sub))]
     (when (realized? sub)
       ;; When subscribe is called concurrently for the same receiver,
       ;; swap! might be called multiple times and end up not using our
       ;; newly created subscription.  Clean up when that happens.
       (let [ch (:ch @sub)]
         (when-not (= ch (registry/recv-ch reg-after f opts))
           (async/close! ch))))
     nil)))

(defn unsubscribe
  "Unsubscribes a function or channel.
   If a `topic` is given, unsubscribes only from this topic.  Otherwise,
   unsubscribes from all topics, i.e. clears all subscriptions from
   [[subscribe]].

   If `f` is not a subscriber, this is a no-op.  Returns `nil`."
  ([broker f]
   (swap! (::registry broker) registry/remove-recv f)
   nil)
  ([broker topic f]
   (swap! (::registry broker) registry/remove-recv-topics f (topics->set topic))
   nil))

(defn unsubscribe-all
  "Unsubscribes all subscribers.
   When a `topic` is given, only subscribers to the given topic will be
   unsubscribed.  Returns `nil`."
  ([broker]
   (swap! (::registry broker) registry/remove-all)
   nil)
  ([broker topic]
   (swap! (::registry broker) registry/remove-topics (topics->set topic))
   nil))
