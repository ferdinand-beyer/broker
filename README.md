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

(broker/publish! broker [:broker-started])
```

By default, messages are expected to be vectors, with the first element being
a keyword to represent the type of message.  This is the same structure as
used by libraries such as [re-frame] and [sente].

The broker uses the message type as a _topic_, and allows subscribers to select
which messages they are interested in.  To subscribe to messages,
use `subscribe` and `subscribe-all`:

```clojure
(defn notify-hello [[_ name]]]
  (println "Hello," (:name name)))

(broker/subscribe broker :hello notify-hello)

(broker/publish! broker [:hello "Broker!"])
; Hello, Broker!

(broker/subscribe-all broker #(println "Observed:" %))

(broker/publish! broker [:hello "All!"])
; Observed: [:hello "All!"]
; Hello, All!

(broker/publish! broker [:hi "what's up?"])
; Observed: [:hi "what's up?"]
```

To customize the message format, you can supply a `:topic-fn`:

```clojure
;; Dispatch on the :msg/topic key instead:
(def broker (broker/start {:topic-fn :msg/topic}))

(broker/publish! broker {:msg/topic :my.domain/widget-created})
```


Under the hood, `broker` uses [`core.async`][core.async].  Instead of
functions, you can also subscribe with async channels:

```clojure
(require '[clojure.core.async :as async])

(def ch (async/chan))

(broker/subscribe broker :hi ch)
```

You can stop receiving messages with `unsubscribe` and release all
subscriptions with `unsubscribe-all`:

```clojure
;; Unsubscribe a channel from a topic:
(broker/unsubscribe broker :hi ch)

;; Unsubscribe a function from all topics:
(broker/unsubscribe broker notify-hello)

;; Remove all subscriptions:
(broker/unsubscribe-all broker)
```

You can stop the router with `stop!`:

```clojure
(broker/stop! broker)

;; This has no effect:
(broker/publish! broker [:more]) ; => false
```

## License

Distributed under the [MIT License].  
Copyright &copy; 2022 [Ferdinand Beyer]

[core.async]: https://github.com/clojure/core.async
[clojars]: https://clojars.org/com.fbeyer/broker
[cljdoc]: https://cljdoc.org/jump/release/com.fbeyer/broker
[re-frame]: https://github.com/day8/re-frame
[sente]: https://github.com/ptaoussanis/sente

[Ferdinand Beyer]: https://fbeyer.com
[MIT License]: https://opensource.org/licenses/MIT
