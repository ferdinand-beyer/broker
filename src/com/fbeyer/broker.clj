(ns com.fbeyer.broker
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.set :as set]))

;; --- Store ---

(def ^:private empty-store
  {:next-id  0
   :data     {}
   :by-topic {}
   :by-recp  {}})

(defn- get-sub [store id]
  (get-in store [:data id]))

(def ^:private set-conj (fnil conj #{}))

(defn- set-assoc [m k v]
  (update m k set-conj v))

(defn- set-dissoc [m k v]
  (let [s (disj (get m k) v)]
    (if (seq s)
      (assoc m k s)
      (dissoc m k))))

(defn- update-topics [store topics f & args]
  (update store :by-topic
          (fn [tmap]
            (reduce (fn [m k] (apply f m k args))
                    tmap topics))))

(defn- add-sub [store {:keys [recp topics] :as sub}]
  (let [id (:next-id store)]
    (-> store
        (update :next-id inc)
        (assoc-in [:data id] (assoc sub :id id))
        (update-in [:by-recp recp] set-conj id)
        (update-topics topics set-assoc id))))

(defn- add-sub-topics [store id topics]
  (-> store
      (update-in [:data id :topics] set/union topics)
      (update-topics topics set-assoc id)))

(defn- remove-sub [store id]
  (let [{:keys [recp topics]} (get-in store [:data id])]
    (-> store
        (update :data dissoc id)
        (update :by-recp set-dissoc recp id)
        (update-topics topics set-dissoc id))))

(defn- remove-sub-topics [store id topics]
  (-> store
      (update-in [:data id :topics] set/difference topics)
      (update-topics topics set-dissoc id)))

(defn- all-subs [store]
  (-> store :data vals))

(defn- recp-subs [store recp]
  (->> (get-in store [:by-recp recp])
       (map (partial get-sub store))))

(defn- recp-topics [store recp]
  (->> (recp-subs store recp)
       (mapcat :topics)
       (into #{})))

(defn- find-recp-sub [store recp opts]
  (->> (recp-subs store recp)
       (filter #(= opts (:opts %)))
       first))

(defn- topic-subs [store topic]
  (->> (get-in store [:by-topic topic])
       (map (partial get-sub store))))

(defn- matching-subs [store topics]
  (->> (mapcat #(get-in store [:by-topic %]) topics)
       (into #{})
       (map (partial get-sub store))))

;; --- Executors ---

(defn- drop-all [rf]
  (fn
    ([] (rf))
    ([result] (rf result))
    ([result _] result)))

(defn- null-chan []
  (async/chan (async/sliding-buffer 1) drop-all))

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

;; --- Control ---

(defn- make-sub [broker recp opts topics]
  (let [sub {:recp   recp
             :opts   opts
             :topics topics}]
    (if (write-port? recp)
      (do (assert (nil? opts))
          (assoc sub :ch recp))
      (let [ch      (make-chan broker opts)
            exec-ch (make-exec broker ch recp opts)]
        (assoc sub :ch ch :exec-ch exec-ch)))))

(defn- stop-sub [{:keys [ch exec-ch]}]
  (when exec-ch
    (async/close! ch)))

(defn- unsub-topics
  [{::keys [store]} {:keys [id] :as sub} topics]
  (let [sub-topics   (:topics sub)
        unsub-topics (set/intersection sub-topics topics)]
    (if (= unsub-topics sub-topics)
      (do (stop-sub sub)
          (swap! store remove-sub id))
      (swap! store remove-sub-topics id unsub-topics))))

(defn- unsub-recp-topics
  [{::keys [store] :as broker} recp topics]
  (doseq [sub (recp-subs @store recp)]
    (unsub-topics broker sub topics)))

(defmulti ^:private -handle-cmd (fn [_broker cmd] (first cmd)))

(defmethod -handle-cmd :subscribe
  [{::keys [store] :as broker} [_ recp opts topics]]
  (if-let [{:keys [id] :as sub} (find-recp-sub @store recp opts)]
    (let [new-topics (set/difference topics (:topics sub))]
      (when (seq new-topics)
        (unsub-recp-topics broker recp new-topics)
        (swap! store add-sub-topics id new-topics)))
    (let [sub (make-sub broker recp opts topics)]
      (unsub-recp-topics broker recp topics)
      (swap! store add-sub sub))))

(defmethod -handle-cmd :remove-closed-sub
  [{::keys [store]} [_ id]]
  (swap! store remove-sub id))

(defmethod -handle-cmd :unsubscribe-recp
  [{::keys [store] :as broker} [_ recp]]
  (let [topics (recp-topics @store recp)]
    (unsub-recp-topics broker recp topics)))

(defmethod -handle-cmd :unsubscribe-recp-topics
  [broker [_ recp topics]]
  (unsub-recp-topics broker recp topics))

(defmethod -handle-cmd :unsubscribe-all
  [{::keys [store]} _]
  (doseq [sub (all-subs @store)]
    (stop-sub sub))
  (reset! store empty-store))

(defmethod -handle-cmd :unsubscribe-topics
  [{::keys [store] :as broker} [_ topics]]
  (doseq [topic topics
          sub   (topic-subs @store topic)]
    (unsub-topics broker sub topics)))

(defmethod -handle-cmd :sync [_ [_ out]]
  (async/put! out true))

(defn- run-control [{::keys [ctrl-ch] :as broker}]
  (async/go-loop []
    (when-some [cmd (async/<! ctrl-ch)]
      (-handle-cmd broker cmd)
      (recur))))

(defn- send-cmd [{::keys [ctrl-ch]} cmd]
  (let [ch (async/chan 1)]
    (and (async/>!! ctrl-ch cmd)
         (async/>!! ctrl-ch [:sync ch])
         (async/<!! ch))))

;; --- Dispatch ---

(defn- run-dispatch [{::keys [dispatch-ch ctrl-ch topic-fn store]}]
  (let [todo-cnt  (atom nil)
        done      (async/chan 1)
        delivered (fn [_] (when (zero? (swap! todo-cnt dec))
                            (async/put! done true)))]
    (async/go-loop []
      (if-some [msg (async/<! dispatch-ch)]
        (let [topic (topic-fn msg)
              subs  (matching-subs @store [::all topic])]
          (when (seq subs)
            (reset! todo-cnt (count subs))
            (doseq [{:keys [id ch]} subs]
              (when-not (async/put! ch msg delivered)
                (async/put! ctrl-ch [:remove-closed-sub id])))
            (async/<! done))
          (recur))
        (doseq [{:keys [ch]} (all-subs @store)]
          (async/close! ch))))))

;; --- Broker ---

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
   (let [broker {::dispatch-ch (async/chan (:buf-or-n opts 1024)
                                           (:xform opts))
                 ::ctrl-ch     (async/chan)
                 ::stop-ch     (async/chan)
                 ::store       (atom empty-store)
                 ::topic-fn    (:topic-fn opts first)
                 ::buf-fn      (:buf-fn opts (constantly 8))
                 ::error-fn    (:error-fn opts thread-uncaught-exc-handler)
                 ::exec        (:exec opts :sequential)}]
     ;; TODO: Save those channels (e.g. instead of stop channel)?
     (run-control broker)
     (run-dispatch broker)
     broker)))

(defn- exec-chans [{::keys [store]}]
  (keep :exec-ch (all-subs @store)))

(defn stop-chan
  "Returns a channel that will close when the broker stops and all
   pending messages are processed by the subscribers.  Can be used
   to block for a graceful shutdown."
  [broker]
  (async/go
    (async/<! (::stop-ch broker))
    (let [ch (async/merge (exec-chans broker))]
      (loop []
        (when (async/<! ch)
          (recur))))))

(defn stop!
  "Stops the broker, closing all internal async channels.
   After that, the broker will no longer accept messages.  Any priorly
   published messages will still be delivered.  Can be called multiple
   times.  Returns `nil`."
  [broker]
  (doseq [k [::ctrl-ch ::dispatch-ch ::stop-ch]]
    (async/close! (broker k))))

(defn shutdown!
  "Stops the broker and waits for all messages to be processed, or `timeout-ms`
   to pass.  Returns `true` when successfully stopped, or `false` on timeout."
  ([broker] (shutdown! broker 60000))
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

   The caller must not close the returned channel."
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
   (send-cmd broker [:subscribe f opts (topics->set topic)])))

(defn unsubscribe
  "Unsubscribes a function or channel.
   If a `topic` is given, unsubscribes only from this topic.  Otherwise,
   unsubscribes from all topics, i.e. clears all subscriptions from
   [[subscribe]].

   If `f` is not a subscriber, this is a no-op.  Returns `nil`."
  ([broker f]
   (send-cmd broker [:unsubscribe-recp f]))
  ([broker topic f]
   (send-cmd broker [:unsubscribe-recp-topics f (topics->set topic)])))

(defn unsubscribe-all
  "Unsubscribes all subscribers.
   When a `topic` is given, only subscribers to the given topic will be
   unsubscribed.  Returns `nil`."
  ([broker]
   (send-cmd broker [:unsubscribe-all]))
  ([broker topic]
   (send-cmd broker [:unsubscribe-topics (topics->set topic)])))
