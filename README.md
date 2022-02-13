# broker

[![Clojars Project](https://img.shields.io/clojars/v/com.fbeyer/broker.svg)][clojars]
[![](https://cljdoc.org/badge/com.fbeyer/broker)][cljdoc]

Simple asynchronous in-process event broker for Clojure.

## Documentation

* [API Docs][cljdoc]

## Latest version

Available from [Clojars][clojars].


## Usage

Start a broker and publish a message:

```clojure
(require '[com.fbeyer.broker :as broker])

(def broker (broker/start))

(broker/publish! broker {:key :broker-started
                         :broker broker})
```

Messages are partitioned by topic.  The topic of a message is determined by
a `:topic-fn` which can be passed to `start`.  When no `:topic-fn` is
configured, messages' topic will be grouped by their `:key` key.

```clojure
(def broker (broker/start {:topic-fn :key}))
```

To subscribe to messages, use `subscribe` and `subscribe-all`:

```clojure
(defn notify-hello [msg]
  (println "Hello," (:name msg)))

(broker/subscribe broker :hello notify-hello)

(broker/publish! broker {:key :hello
                         :name "Broker!"})
; Hello, Broker!

(broker/subscribe-all broker #(println "Observed:" %))

(broker/publish! broker {:key :hello
                         :name "All!"})
; Observed: {:key :hello, :name All!}
; Hello, All!

(broker/publish! broker {:key :goodbye})
; Observed: {:key :goodbye}
```

Under the hood, `broker` uses [`core.async`][core.async].  Instead of
functions, you can also subscribe with async channels:

```clojure
(require '[clojure.core.async :as async])

(def ch (async/chan))

(broker/subscribe broker :goodbye ch)
```

You can stop receiving messages with `unsubscribe` and release all
subscriptions with `unsubscribe-all`:

```clojure
;; Unsubscribe a channel from a topic:
(broker/unsubscribe broker :goodbye ch)

;; Unsubscribe a function from all topics:
(broker/unsubscribe broker notify-hello)

;; Remove all subscriptions:
(broker/unsubscribe-all broker)
```

You can stop the router with `stop!`:

```clojure
(broker/stop! broker)

;; This has no effect:
(broker/publish! broker {:key :more}) ; => false
```

## License

Distributed under the [MIT License].  
Copyright &copy; 2022 [Ferdinand Beyer]

[core.async]: https://github.com/clojure/core.async
[clojars]: https://clojars.org/com.fbeyer/broker
[cljdoc]: https://cljdoc.org/jump/release/com.fbeyer/broker

[Ferdinand Beyer]: https://fbeyer.com
[MIT License]: https://opensource.org/licenses/MIT
