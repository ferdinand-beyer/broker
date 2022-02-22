(ns com.fbeyer.broker
  "A simple in-process message/event broker for Clojure."
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]))

(defn thread-uncaught-exc-handler
  "Default `:error-fn` for [[start]], passes exceptions to the current
   thread's `UncaughtExceptionHandler`."
  [e _]
  (let [thread (Thread/currentThread)]
    (-> (.getUncaughtExceptionHandler thread)
        (.uncaughtException thread e)))
  nil)

(defn- drop-all [rf]
  (fn
    ([] (rf))
    ([result] (rf result))
    ([result _] result)))

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
     passed to the current thread's `UncaughtExceptionHandler`."
  ([] (start nil))
  ([opts]
   (let [source (async/chan (:buf-or-n opts 1024)
                            (:xform opts))
         mult   (async/mult source)]
     {::source   source
      ::sink     (async/chan (async/sliding-buffer 1) drop-all)
      ::stop     (async/chan)
      ::mult     mult
      ::pub      (-> (async/tap mult (async/chan)) ; unbuffered - ok?
                     (async/pub (:topic-fn opts first)))
      ::subs     (atom {})
      ::buf-fn   (:buf-fn opts (constantly 8))
      ::error-fn (:error-fn opts thread-uncaught-exc-handler)})))

(defn- pipeline-channels [{::keys [subs]}]
  (->> @subs vals (keep #(nth % 2))))

(defn stop-chan
  "Returns a channel that will close when the broker stops and all
   pending messages are processed by the subscribers.  Can be used
   to block for a graceful shutdown."
  [broker]
  (async/go
    (async/<! (::stop broker))
    (let [ch (async/merge (pipeline-channels broker))]
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

(defn- write-port? [x]
  (satisfies? async-protocols/WritePort x))

(defn- ensure-unique-key [subs k]
  (if (contains? subs k)
    (throw (ex-info "subscription key already exists in broker" {:key k}))
    k))

(defn- subscriber-channel [{::keys [buf-fn]} {:keys [buf-or-n]}]
  (async/chan (or buf-or-n (buf-fn))))

(defn- wrap-function [{::keys [error-fn] :as broker} f]
  (fn [msg]
    (try
      (f msg)
      (catch Throwable e
        (error-fn e {:broker broker
                     :fn f
                     :msg msg})))))

(defn- pipeline
  [{::keys [sink] :as broker} ch f {:keys [blocking? parallel]
                                    :or {blocking? true}}]
  (let [f (wrap-function broker f)]
    (if (some? parallel)
      (if blocking?
        (async/pipeline-blocking parallel sink (keep f) ch false)
        (async/pipeline-async parallel sink
                              (fn [msg out]
                                (f msg)
                                (async/close! out))
                              ch false))
      (if blocking?
        (async/go-loop [cs [ch]]
          (when (seq cs)
            (let [[v c] (async/alts! cs)]
              (if (and (= c ch) (some? v))
                (recur (conj cs (async/thread (f v))))
                (recur (filterv #(not= % c) cs))))))
        (async/go-loop []
          (when-let [msg (async/<! ch)]
            (f msg)
            (recur)))))))

(defn subscribe
  "Subscribes to messages from the broker.

   When a `topic` is given, subscribes only to messages of this topic.
   `topic` can also be a sequence of multiple topics to subscribe to.
   Without `topic`, subscribes to all messages.

   If `f` is a channel, it is the caller's responsibility to block
   a non-daemon thread to ensure the message is consumed.

   Options:
   * `:key` - key to unsubscribe (default: `f`)
   * `:blocking?` - whether `f` might block (default: `true`)
   * `:parallel` - how many parallel calls to allow (default: `nil` - unbounded)
   * `:buf-or-n` - buffer for this subscription.  Defaults to the `buf-fn`
      passed to [[start]]."
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
   (let [{::keys [mult pub subs]} broker
         k  (ensure-unique-key @subs (:key opts f)) ; can we relax that?
         ch (if (write-port? f) f (subscriber-channel broker opts))
         p  (when-not (write-port? f) (pipeline broker ch f opts))]
     (if (some? topic)
       (let [ts (if (sequential? topic) (set topic) #{topic})]
         (doseq [topic ts]
           (async/sub pub topic ch))
         (swap! subs assoc k [ch ts p]))
       (do (async/tap mult ch)
           (swap! subs assoc k [ch nil p])))
     nil)))

(defn- remove-sub [{::keys [subs]} k ch p]
  (when p
    (async/close! ch))
  (swap! subs dissoc k))

(defn- unsub [{::keys [pub subs] :as broker} topic k [ch topics p]]
  (when (contains? topics topic)
    (let [ts (disj topics topic)]
      (async/unsub pub topic ch)
      (if (seq ts)
        (swap! subs assoc k [ch topics p])
        (remove-sub broker k ch p)))))

(defn unsubscribe
  "Unsubscribes a function or channel.
   If a `topic` is given, unsubscribes only from this topic.  Otherwise,
   unsubscribes from all topics, i.e. clears all subscriptions from
   [[subscribe]].

   If `k` is not a subscriber, this is a no-op.  Returns `nil`."
  ([broker k]
   (let [{::keys [mult pub subs]} broker]
     (when-let [[ch topics p] (get @subs k)]
       (if topics
         (doseq [topic topics]
           (async/unsub pub topic ch))
         (async/untap mult ch))
       (remove-sub broker k ch p)
       nil)))
  ([broker topic k]
   (let [{::keys [subs]} broker]
     (when-let [s (get @subs k)]
       (unsub broker topic k s)
       nil))))

(defn unsubscribe-all
  "Unsubscribes all subscribers.
   When a `topic` is given, only subscribers to the given topic will be
   unsubscribed.  Returns `nil`."
  ([broker]
   (let [{::keys [mult pub subs]} broker]
     (async/unsub-all pub)
     (doseq [[_ [ch topics _]] @subs]
       (when (nil? topics)
         (async/untap mult ch)))
     (reset! subs {})
     nil))
  ([broker topic]
   (let [{::keys [subs]} broker]
     (doseq [[k s] @subs]
       (unsub broker topic k s))
     nil)))
