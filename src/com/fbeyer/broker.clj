(ns com.fbeyer.broker
  "A simple in-process message/event broker for Clojure."
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.set :as set]))

(def ^:private empty-subdb
  {:next-id 0
   :id      {}
   :target  {}
   :topic   {}})

(def ^:private set-conj (fnil conj #{}))

(defn- set-assoc [m k v]
  (update m k set-conj v))

(defn- set-dissoc [m k v]
  (let [s (disj (get m k) v)]
    (if (seq s)
      (assoc m k s)
      (dissoc m k))))

(defn- update-topics [subdb topics f & args]
  (update subdb :topic (fn [tmap]
                         (reduce (fn [m k] (apply f m k args))
                                 tmap topics))))

(defn- add-sub [subdb {:keys [target topics] :as sub}]
  (let [id (:next-id subdb)]
    (-> subdb
        (update :next-id inc)
        (assoc-in [:id id] (assoc sub :id id))
        (update-in [:target target] set-conj id)
        (update-topics topics set-assoc id))))

(defn- add-sub-topics [subdb id topics]
  (-> subdb
      (update-in [:id id :topics] set/union topics)
      (update-topics topics set-assoc id)))

(defn- remove-sub [subdb id]
  (let [{:keys [target topics]} (get-in subdb [:id id])]
    (-> subdb
        (update :id dissoc id)
        (update :target set-dissoc target id)
        (update-topics topics set-dissoc id))))

(defn- remove-sub-topics [subdb id topics]
  (-> subdb
      (update-in [:id id :topics] set/difference topics)
      (update-topics topics set-dissoc id)))

(defn- all-subs [subdb]
  (-> subdb :id vals))

(defn- target-subs [subdb target]
  (->> (get-in subdb [:target target])
       (map #(get-in subdb [:id %]))))

(defn- target-topics [subdb target]
  (->> (target-subs subdb target)
       (mapcat :topics)
       (into #{})))

(defn- topic-subs [subdb topic]
  (->> (get-in subdb [:topic topic])
       (map #(get-in subdb [:id %]))))

(defn- find-compatible [subdb target opts]
  (->> (target-subs subdb target)
       (filter #(= opts (:opts %)))
       first))

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
   (let [source (async/chan (:buf-or-n opts 1024)
                            (:xform opts))
         mult   (async/mult source)]
     {::source   source
      ::stop     (async/chan)
      ::mult     mult
      ::pub      (-> (async/tap mult (async/chan)) ; unbuffered - ok?
                     (async/pub (:topic-fn opts first)))
      ::subdb    (atom empty-subdb)
      ::buf-fn   (:buf-fn opts (constantly 8))
      ::error-fn (:error-fn opts thread-uncaught-exc-handler)
      ::exec     (:exec opts :sequential)})))

(defn- exec-chans [{::keys [subdb]}]
  (keep :exec-ch (all-subs @subdb)))

(defn stop-chan
  "Returns a channel that will close when the broker stops and all
   pending messages are processed by the subscribers.  Can be used
   to block for a graceful shutdown."
  [broker]
  (async/go
    (async/<! (::stop broker))
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
  (async/close! (::source broker))
  (async/close! (::stop broker)))

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
  (async/>!! (::source broker) msg))

(defn publish-chan
  "Returns the channel used for publishing messages to the broker.

   Intended for publishing from a `(go ...)` block or advanced usage
   such as bulk publishing / piping into the broker.

   The caller must not close the returned channel."
  [broker]
  (::source broker))

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

(defn- make-sub [broker topics f opts]
  (let [sub {:topics topics
             :target f
             :opts   opts}]
    (if (write-port? f)
      (do (assert (nil? opts))
          (assoc sub :ch f))
      (let [ch      (make-chan broker opts)
            exec-ch (make-exec broker ch f opts)]
        (assoc sub :ch ch :exec-ch exec-ch)))))

(defn- sub-ch [{::keys [mult pub]} topics ch]
  (doseq [topic topics]
    (if (= ::all topic)
      (async/tap mult ch)
      (async/sub pub topic ch))))

(defn- unsub-ch [{::keys [mult pub]} topics ch]
  (doseq [topic topics]
    (if (= ::all topic)
      (async/untap mult ch)
      (async/unsub pub topic ch))))

(defn- close-sub! [{:keys [ch exec-ch]}]
  (when exec-ch
    (async/close! ch)))

(defn- delete-sub!
  [{::keys [subdb] :as broker} {:keys [id topics ch] :as sub}]
  (unsub-ch broker topics ch)
  (close-sub! sub)
  (swap! subdb remove-sub id))

(defn- unsub-topics
  [{::keys [subdb] :as broker} {:keys [id ch] :as sub} topics]
  (let [sub-topics   (:topics sub)
        unsub-topics (set/intersection sub-topics topics)]
    (if (= unsub-topics sub-topics)
      (delete-sub! broker sub)
      (do (unsub-ch broker unsub-topics ch)
          (swap! subdb remove-sub-topics id unsub-topics)))))

(defn- unsub-target-topics
  [{::keys [subdb] :as broker} topics f]
  (doseq [sub (target-subs @subdb f)]
    (unsub-topics broker sub topics)))

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
   (let [topics (topics->set topic)
         subdb  (::subdb broker)]
     (if-let [{:keys [id ch] :as sub} (find-compatible @subdb f opts)]
       (let [new-topics (set/difference topics (:topic sub))]
         (when (seq new-topics)
           (unsub-target-topics broker new-topics f)
           (swap! subdb add-sub-topics id new-topics)
           (sub-ch broker new-topics ch)))
       (let [{:keys [ch] :as sub} (make-sub broker topics f opts)]
         (unsub-target-topics broker topics f)
         (swap! subdb add-sub sub)
         (sub-ch broker topics ch))))
   nil))

(defn unsubscribe
  "Unsubscribes a function or channel.
   If a `topic` is given, unsubscribes only from this topic.  Otherwise,
   unsubscribes from all topics, i.e. clears all subscriptions from
   [[subscribe]].

   If `f` is not a subscriber, this is a no-op.  Returns `nil`."
  ([broker f]
   (let [subdb  (::subdb broker)
         topics (target-topics @subdb f)]
     (unsub-target-topics broker topics f)))
  ([broker topic f]
   (unsub-target-topics broker (topics->set topic) f)))

(defn unsubscribe-all
  "Unsubscribes all subscribers.
   When a `topic` is given, only subscribers to the given topic will be
   unsubscribed.  Returns `nil`."
  ([broker]
   (let [{::keys [mult pub subdb]} broker]
     (async/unsub-all pub)
     (doseq [{:keys [ch topics] :as sub} (all-subs @subdb)]
       (when (contains? topics ::all)
         (async/untap mult ch))
       (close-sub! sub))
     (reset! subdb empty-subdb)
     nil))
  ([broker topic]
   (let [subdb  (::subdb broker)
         topics (topics->set topic)]
     (doseq [topic topics
             sub   (topic-subs @subdb topic)]
       (unsub-topics broker sub topics)))))

(comment
  ;; TODO: Reproducing example:
  (dotimes [n 100]
    (let [start  (async/chan)
          pub    (async/pub start first)
          end    (async/chan)
          _      (async/sub pub :a end)
          _      (async/sub pub :b end)
          recv   (atom #{})
          done   (async/go-loop []
                   (when-let [v (async/<! end)]
                     (swap! recv conj v)
                     (recur)))
          sent   [[:a 1] [:a 2] [:b 3]]]
      (doseq [v sent]
        (async/>!! start v))
      (async/close! start)
      (async/<!! done)
      (let [exp (set sent)
            act (set @recv)]
        (when (not= exp act)
          (println "No." n)
          (println "Expected:" exp)
          (println "Actual:  " act)
          (assert false)))))

  )
