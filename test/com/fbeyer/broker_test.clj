(ns com.fbeyer.broker-test
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-impl]
            [clojure.test :refer [deftest is testing]]
            [com.fbeyer.broker :as broker]))

(defn- take!! [ch]
  (async/alt!!
    ch ([msg] msg)
    (async/timeout 100) ::timeout))

(deftest subscribe-all-test
  (testing "subscribing to all messages with a channel"
    (let [broker (broker/start)
          msg    [::subscribe-all-ch]
          ch     (async/chan)]
      (broker/subscribe-all broker ch)
      (broker/publish! broker msg)
      (is (= msg (take!! ch)))))

  (testing "subscribing to all messages with a callback function"
    (let [broker (broker/start)
          msg    [::subscribe-all-fn]
          p      (promise)]
      (broker/subscribe-all broker (partial deliver p))
      (broker/publish! broker msg)
      (is (= msg (deref p 100 ::timeout))))))

(deftest subscribe-test
  (testing "only receive messages from subscribed topic"
    (let [broker (broker/start)
          ch     (async/chan)]
      (broker/subscribe broker ::wanted ch)
      (broker/publish! broker [::unwanted-1])
      (broker/publish! broker [::unwanted-2])
      (broker/publish! broker [::wanted])
      (broker/publish! broker [::unwanted-3])
      (is (= [::wanted] (take!! ch))))))

(deftest topic-fn-test
  (let [broker (broker/start {:topic-fn ::topic})
        ch     (async/chan)]
    (broker/subscribe broker ::custom-topic-fn ch)
    (broker/publish! broker {::topic ::custom-topic-fn})
    (is (= {::topic ::custom-topic-fn} (take!! ch)))))

(deftest error-handler-test
  (let [p        (promise)
        error-fn (fn [e ctx]
                   (deliver p [e ctx]))
        broker   (broker/start {:error-fn error-fn})
        exc      (ex-info "error test" {})
        f        (fn [_] (throw exc))
        msg      [::error-handler-test]]
    (broker/subscribe-all broker f)
    (broker/publish! broker msg)
    (let [[e ctx] (deref p 100 nil)]
      (is (= exc e))
      (is (= broker (:broker ctx)))
      (is (= f (:fn ctx)))
      (is (= msg (:msg ctx))))))

(deftest stop!-test
  (let [broker (broker/start)
        ch (async/chan)]
    (broker/subscribe-all broker ch)
    (broker/publish! broker [::delivered])
    (broker/stop! broker)
    (broker/publish! broker [::dropped])
    (is (= [::delivered] (take!! ch)) "published messages are still delivered")
    (is (nil? (take!! ch)))
    (is (async-impl/closed? ch) "subscribed channels are closed")))

(deftest unsubscribe-test
  (testing "unsubscribe from a topic"
    (let [broker (broker/start)
          ch (async/chan 2)]
      (broker/subscribe broker ::spam ch)
      (broker/subscribe broker ::eggs ch)
      (broker/unsubscribe broker ::spam ch)
      (broker/publish! broker [::spam])
      (broker/publish! broker [::eggs])
      (is (= [::eggs] (take!! ch)))
      (broker/stop! broker)
      (is (nil? (take!! ch)))))

  (testing "unsubscribe from all topics"
    (let [broker (broker/start)
          ch (async/chan 1)]
      (broker/subscribe-all broker ch)
      (broker/subscribe broker ::news ch)
      (broker/unsubscribe broker ch)
      (broker/publish! broker [::news])
      (broker/stop! broker)
      (is (= ::timeout (take!! ch)))))

  (testing "unsubscribe all consumers from a topic"
    (let [broker (broker/start)
          ch1 (async/chan 1)
          ch2 (async/chan 1)
          ch3 (async/chan 1)]
      (broker/subscribe broker ::topic ch1)
      (broker/subscribe broker ::topic ch2)
      (broker/subscribe-all broker ch3)
      (broker/unsubscribe-all broker ::topic)
      (broker/publish! broker [::topic])
      (is (= ::timeout (take!! ch1)))
      (is (= ::timeout (take!! ch2)))
      (is (= [::topic] (take!! ch3)) "all-consumers are still subscribed")))

  (testing "unsubscribe all consumers"
    (let [broker (broker/start)
          ch1 (async/chan 1)
          ch2 (async/chan 1)]
      (broker/subscribe broker ::topic ch1)
      (broker/subscribe-all broker ch2)
      (broker/unsubscribe-all broker)
      (broker/publish! broker [::topic])
      (is (= ::timeout (take!! ch1)))
      (is (= ::timeout (take!! ch2))))))
