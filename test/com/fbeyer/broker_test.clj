(ns com.fbeyer.broker-test
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-impl]
            [clojure.test :refer [deftest is testing]]
            [com.fbeyer.broker :as broker]))

(def timeout-ms 100)

(defn- take!! [ch]
  (async/alt!!
    ch ([msg] msg)
    (async/timeout timeout-ms) ::timeout))

(deftest subscribe-test
  (testing "subscribing to all messages with a callback function"
    (let [broker (broker/start)
          msg    [::subscribe-all-fn]
          p      (promise)]
      (broker/subscribe broker (partial deliver p))
      (broker/publish! broker msg)
      (is (= msg (deref p timeout-ms ::timeout)))))

  (testing "subscribing to all messages with a channel"
    (let [broker (broker/start)
          msg    [::subscribe-all-ch]
          ch     (async/chan)]
      (broker/subscribe broker ch)
      (broker/publish! broker msg)
      (is (= msg (take!! ch)))))

  (testing "only receive messages from subscribed topic"
    (let [broker (broker/start)
          p      (promise)]
      (broker/subscribe broker ::wanted (partial deliver p))
      (broker/publish! broker [::unwanted-1])
      (broker/publish! broker [::unwanted-2])
      (broker/publish! broker [::wanted])
      (broker/publish! broker [::unwanted-3])
      (is (= [::wanted] (deref p timeout-ms ::timeout)))))

  (testing "subscribing to multiple topics"
    (let [broker (broker/start)
          msgs   (atom [])]
      (broker/subscribe broker [::topic1 ::topic2] (partial swap! msgs conj))
      (broker/publish! broker [::topic1 "first"])
      (broker/publish! broker [::topic2 "second"])
      (broker/publish! broker [::topic3 "third"])
      (broker/publish! broker [::topic1 "fourth"])
      (broker/shutdown! broker)
      (is (= [[::topic1 "first"] [::topic2 "second"] [::topic1 "fourth"]]
             @msgs))))

  (testing "updating to additional topics"
    (let [broker (broker/start)
          msgs   (atom [])
          f      (partial swap! msgs conj)]
      (broker/subscribe broker ::a f)
      (broker/subscribe broker ::b f)
      (broker/publish! broker [::a])
      (broker/publish! broker [::b])
      (broker/shutdown! broker)
      (is (= [[::a] [::b]] @msgs)))))

(deftest publish-chan-test
  (let [broker (broker/start)
        p      (promise)]
    (broker/subscribe broker (partial deliver p))
    (async/put! (broker/publish-chan broker) [::put])
    (is (= [::put] (deref p timeout-ms ::timeout)))))

(deftest topic-fn-test
  (let [broker (broker/start {:topic-fn ::topic})
        p      (promise)]
    (broker/subscribe broker ::custom-topic-fn (partial deliver p))
    (broker/publish! broker {::topic ::custom-topic-fn})
    (is (= {::topic ::custom-topic-fn} (deref p timeout-ms ::timeout)))))

(deftest default-error-fn-test
  (let [thread   (Thread/currentThread)
        orig     (.getUncaughtExceptionHandler thread)
        p        (promise)
        handler  (reify Thread$UncaughtExceptionHandler
                   (uncaughtException [_ _ e] (deliver p e)))
        broker   (broker/start)
        error-fn (::broker/error-fn broker)
        exc      (ex-info "error test" {})]
    (try
      (.setUncaughtExceptionHandler thread handler)
      (error-fn exc nil)
      (finally
        (.setUncaughtExceptionHandler thread orig)))
    (let [e (deref p timeout-ms ::timeout)]
      (is (identical? exc e)
          "passes exception to thread's uncaught exception handler"))))

(deftest error-handler-test
  (let [p        (promise)
        error-fn (fn [e ctx]
                   (deliver p [e ctx]))
        broker   (broker/start {:error-fn error-fn})
        exc      (ex-info "error test" {})
        f        (fn [_] (throw exc))
        msg      [::error-handler-test]]
    (broker/subscribe broker f)
    (broker/publish! broker msg)
    (let [[e ctx] (deref p timeout-ms nil)]
      (is (= exc e))
      (is (= broker (:broker ctx)))
      (is (= f (:fn ctx)))
      (is (= msg (:msg ctx))))))

(deftest stop!-test
  (let [broker (broker/start)
        ch     (async/chan)]
    (broker/subscribe broker ch)
    (broker/publish! broker [::delivered])
    (broker/stop! broker)
    (broker/publish! broker [::dropped])
    (is (= [::delivered] (take!! ch)) "published messages are still delivered")
    (is (nil? (take!! ch)))
    (is (async-impl/closed? ch) "subscribed channels are closed")))

(deftest unsubscribe-test
  (testing "unsubscribe from a topic"
    (let [broker (broker/start)
          ch     (async/chan 2)]
      (broker/subscribe broker [::spam ::eggs] ch)
      (broker/unsubscribe broker ::spam ch)
      (broker/publish! broker [::spam])
      (broker/publish! broker [::eggs])
      (is (= [::eggs] (take!! ch)))
      (broker/stop! broker)
      (is (nil? (take!! ch)))))

  (testing "unsubscribe from all topics"
    (let [broker (broker/start)
          ch     (async/chan 1)]
      (broker/subscribe broker [::news ::olds] ch)
      (broker/unsubscribe broker ch)
      (broker/publish! broker [::news])
      (broker/stop! broker)
      (is (= ::timeout (take!! ch)))))

  (testing "unsubscribe all consumers from a topic"
    (let [broker (broker/start)
          ch1    (async/chan 1)
          ch2    (async/chan 1)
          ch3    (async/chan 1)]
      (broker/subscribe broker ::topic ch1)
      (broker/subscribe broker ::topic ch2)
      (broker/subscribe broker ch3)
      (broker/unsubscribe-all broker ::topic)
      (broker/publish! broker [::topic])
      (is (= ::timeout (take!! ch1)))
      (is (= ::timeout (take!! ch2)))
      (is (= [::topic] (take!! ch3)) "all-consumers are still subscribed")))

  (testing "unsubscribe all consumers"
    (let [broker (broker/start)
          ch1    (async/chan 1)
          ch2    (async/chan 1)]
      (broker/subscribe broker ::topic ch1)
      (broker/subscribe broker ch2)
      (broker/unsubscribe-all broker)
      (broker/publish! broker [::topic])
      (is (= ::timeout (take!! ch1)))
      (is (= ::timeout (take!! ch2))))))

(deftest publish-xform-test
  (let [map->vec (fn [msg] (if (map? msg) [(:topic msg) msg] msg))
        broker   (broker/start {:xform (map map->vec)})
        p        (promise)]
    (broker/subscribe broker (partial deliver p))
    (broker/publish! broker {:topic ::transformed})
    (is (= [::transformed {:topic ::transformed}]
           (deref p timeout-ms ::timeout)))))
