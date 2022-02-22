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
      ::mult     mult
      ::pub      (-> (async/tap mult (async/chan)) ; unbuffered - ok?
                     (async/pub (:topic-fn opts first)))
      ::subs     (atom {})
      ::taps     (atom #{})
      ::fn-chs   (atom {})
      ::buf-fn   (:buf-fn opts (constantly 8))
      ::error-fn (:error-fn opts thread-uncaught-exc-handler)})))

(defn stop!
  "Stops the broker, closing all internal async channels.
   After that, the broker will no longer accept messages.  Any priorly
   published messages will still be delivered.  Can be called multiple
   times.  Returns `nil`."
  [broker]
  (async/close! (::source broker)))

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

   Closing the channel will stop the broker."
  [broker]
  (::source broker))

(defn- write-port? [x]
  (satisfies? async-protocols/WritePort x))

(defn- wrap-errors [{::keys [error-fn] :as broker} f]
  (fn [msg]
    (try
      (f msg)
      (catch Throwable e
        (error-fn e {:broker broker
                     :fn f
                     :msg msg})))))

(defn- make-fn-chan [{::keys [sink buf-fn] :as broker} f]
  (let [ch (async/chan (buf-fn))
        f* (wrap-errors broker f)]
    ;; TODO: Register returned channel
    ;(async/pipeline-async 1 sink (fn [msg out] (async/thread (f* msg) (async/close! out))) ch false)
    (async/go-loop []
      (when-let [msg (async/<! ch)]
        (f* msg)
        ;(async/<! (async/thread (f* msg)))
        (recur)))
    ch))

(defn- fn-chan [{::keys [fn-chs] :as broker} f]
  (or (get @fn-chs f)
      (get (swap! fn-chs update f #(or % (make-fn-chan broker f))) f)))

(defn- ->chan [broker x]
  (cond
    (write-port? x) x
    (fn? x) (fn-chan broker x)
    :else (throw (ex-info "expected a function or channel" {:actual x}))))

(defn subscribe
  "Subscribes a function or channel to a topic.
   If `fn-or-ch` is already subscribed to this topic, this has no effect.

   When `fn-or-ch` is a function, it will be run in an async `(go ...)` block
   and should not block.  Exceptions will be reported to the broker's
   `:error-fn` (see [[start]])."
  [broker topic fn-or-ch]
  (let [{::keys [pub subs]} broker
        ch (->chan broker fn-or-ch)]
    (async/sub pub topic ch)
    (swap! subs update ch (fnil conj #{}) topic)
    nil))

(defn subscribe-all
  "Subscribes a function or channel to all messages published to the broker.
   If `fn-or-ch` is already subscribed to all messages, this has no effect.

   When `fn-or-ch` is also subscribed explicitly to a topic, it will receive
   messages twice for this topic.

   When `fn-or-ch` is a function, it will be run in an async `(go ...)` block
   and should not block.  Exceptions will be reported to the broker's
   `:error-fn` (see [[start]])."
  [broker fn-or-ch]
  (let [{::keys [mult taps]} broker
        ch (->chan broker fn-or-ch)]
    (async/tap mult ch)
    (swap! taps conj ch)
    nil))

(defn- get-sub-chan [x fn-chs]
  (cond
    (write-port? x) x
    (fn? x) (get @fn-chs x)))

(defn- unsub-chan [{::keys [pub subs]} topic ch]
  (async/unsub pub topic ch)
  (swap! subs update ch disj topic))

(defn- unsub-chan-from-all [{::keys [pub subs]} ch]
  (doseq [topic (get @subs ch)]
    (async/unsub pub topic ch))
  (swap! subs dissoc ch))

(defn- unsub-all [{::keys [pub subs]}]
  (async/unsub-all pub)
  (reset! subs {}))

(defn- unsub-all-from-topic [{::keys [pub subs]} topic]
  (async/unsub-all pub topic)
  (swap! subs #(into {} (for [[ch topics] %]
                          [ch (disj topics topic)]))))

(defn- untap-chan [{::keys [mult taps]} ch]
  (async/untap mult ch)
  (swap! taps disj ch))

(defn- untap-all [{::keys [mult taps]}]
  (doseq [ch @taps]
    (async/untap mult ch))
  (reset! taps #{}))

(defn unsubscribe
  "Unsubscribes a function or channel.
   If a `topic` is given, unsubscribes only from this topic.  Otherwise,
   unsubscribes from all topics, i.e. clears all subscriptions from
   [[subscribe]] and [[subscribe-all]].

   If `fn-or-ch` is not a subscriber, this is a no-op.  Returns `nil`."
  ([broker fn-or-ch]
   (when-let [ch (get-sub-chan fn-or-ch (:fn-chs broker))]
     (unsub-chan-from-all broker ch)
     (untap-chan broker ch)
     nil))
  ([broker topic fn-or-ch]
   (when-let [ch (get-sub-chan fn-or-ch (:fn-chs broker))]
     (unsub-chan broker topic ch)
     nil)))

(defn unsubscribe-all
  "Unsubscribes all subscribers.
   When a `topic` is given, only subscribers to the given topic will be
   unsubscribed.  Returns `nil`."
  ([broker]
   (unsub-all broker)
   (untap-all broker)
   nil)
  ([broker topic]
   (unsub-all-from-topic broker topic)
   nil))
