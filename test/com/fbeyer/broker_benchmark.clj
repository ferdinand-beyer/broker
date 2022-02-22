(ns com.fbeyer.broker-benchmark
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is]]
            [com.fbeyer.broker :as broker]))

(defn- num-cpus []
  (.. (Runtime/getRuntime) availableProcessors))

(defn- do-publish [broker topic data n]
  (async/onto-chan! (broker/publish-chan broker)
                    (for [i (range n)]
                      [topic (assoc data :num i)])
                    false))

(defn- make-counter [a]
  (fn [_] (swap! a inc)))

(deftest high-load-benchmark
  (let [num-topics     5
        num-publishers (num-cpus)
        num-messages   10000
        timeout-secs   10
        expected-count (* num-publishers num-messages)
        expected-total (* num-topics expected-count)
        topic-counts   (vec (repeatedly num-topics #(atom 0)))
        total          (atom 0)
        done           (promise)
        broker         (broker/start)
        opts           {:blocking? false}]
    (add-watch total ::done (fn [_ _ _ n]
                              (when (= n expected-total)
                                (deliver done true))))
    (broker/subscribe broker (make-counter total) opts)
    (dotimes [i num-topics]
      (broker/subscribe broker (str "topic-" i)
                        (make-counter (nth topic-counts i))
                        opts))
    (prn  (str "Publishing " expected-total " messages..."))
    (time
     (do
       (dotimes [i num-topics]
         (let [topic (str "topic-" i)]
           (dotimes [k num-publishers]
             (do-publish broker topic {:publisher (str topic "-" k)} num-messages))))
       (is (true? (deref done (* timeout-secs 1000) false)))))
    (is (= expected-total @total))
    (Thread/sleep 100) ; topic subscribers might need a teeny bit longer
    (dotimes [i num-topics]
      (is (= expected-count @(nth topic-counts i))
          (str "received all messages for topic " i)))))

(def ^:private -ns *ns*)

(defn run-benchmark [_opts]
  (let [{:keys [fail error]} (clojure.test/run-tests -ns)]
    (zero? (+ fail error))))

(defn -main [& _args]
  (System/exit (if (run-benchmark nil) 0 1)))
