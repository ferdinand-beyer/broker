(ns com.fbeyer.broker
  "A simple in-process message/event broker for Clojure."
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]))

(defn start
  "Starts a new broker.

   Supported options:
     :topic-fn - function used to determine the topic of an incoming message
                 (default: :key).
     :error-fn - when a subscribing function throws an exception, this function
                 will be called with three arguments: the broker, the function,
                 and the exception.  By default, exceptions will be silently
                 ignored."
  [& [{:keys [topic-fn error-fn]
       :or   {topic-fn :key}}]]
  ;; TODO: Buffers?
  (let [ch   (async/chan)
        mult (async/mult ch)]
    {::ch   ch
     ::mult mult
     ::pub  (-> (async/tap mult (async/chan))
                (async/pub topic-fn))
     ::subs   (atom {})
     ::taps   (atom #{})
     ::fn-chs (atom {})
     ::error-fn error-fn}))

(defn stop!
  "Stops the broker, closing all internal async channels.  After that, the
   broker will no longer accept messages.  Any priorly published messages will
   still be delivered.  Returns nil."
  [broker]
  (async/close! (::ch broker)))

(defn publish!
  "Publishes a message.  All subscribers will be notified asynchronously."
  [broker msg]
  (async/put! (::ch broker) msg))

(defn- write-port? [x]
  (satisfies? async-protocols/WritePort x))

(defn- make-fn-chan [broker f]
  (let [ch (async/chan)] ; TODO: Buffer?
    (async/go-loop []
      (when-let [msg (async/<! ch)]
        (try
          (f msg)
          (catch Throwable e
            (when-let [err-fn (::error-fn broker)]
              (err-fn broker f e))))
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
  "Subscribes a function or channel to a topic.  If fn-or-ch is already
   subscribed to this topic, this is a no-op.

   When fn-or-ch is a function, it will be run in an async (go ...) block and
   should not block.  Exceptions will be reported to the broker's :error-fn
   (see start)."
  [{::keys [pub subs] :as broker} topic fn-or-ch]
  (let [ch (->chan broker fn-or-ch)]
    (async/sub pub topic ch)
    (swap! subs update ch (fnil conj #{}) topic)
    nil))

(defn subscribe-all
  "Subscribes a function or channel to all messages published to the broker.
   When fn-or-ch is already subscribed to all messages, this is a no-op.

   When fn-or-ch is also subscribed explicitly to a topic, it will receive
   messages twice.

   When fn-or-ch is a function, it will be run in an async (go ...) block and
   should not block.  Exceptions will be reported to the broker's :error-fn
   (see start)."
  [{::keys [mult taps] :as broker} fn-or-ch]
  (let [ch (->chan broker fn-or-ch)]
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
  "Unsubscribes a function or channel.  If a topic is given, unsubscribes only
   from this topic.  Otherwise, unsubscribes from all messages.  If fn-or-ch
   has not subscribed to any messages, this function has no effect.
   Returns nil."
  ([{::keys [fn-chs] :as broker} fn-or-ch]
   (when-let [ch (get-sub-chan fn-or-ch fn-chs)]
     (unsub-chan-from-all broker ch)
     (untap-chan broker ch)
     nil))
  ([{::keys [fn-chs] :as broker} topic fn-or-ch]
   (when-let [ch (get-sub-chan fn-or-ch fn-chs)]
     (unsub-chan broker topic ch)
     nil)))

(defn unsubscribe-all
  "Unsubscribes all subscribers.  When a topic is given, only subscribers to
   the given topic will be unsubscribed."
  ([broker]
   (unsub-all broker)
   (untap-all broker)
   nil)
  ([broker topic]
   (unsub-all-from-topic broker topic)
   nil))
