(ns com.fbeyer.broker.impl.registry
  {:skip-wiki true}
  (:require [clojure.set :as set]))

(def empty-registry
  {:cs  {} ; ch->sub
   :roc {} ; recv->opts->ch
   :tc  {} ; topic->ch
   :cs! {} ; ch->sub (removed)
   })

(defn all-subs [registry]
  (-> registry :cs vals))

(defn topics->chans [registry topics]
  (into #{} (mapcat #(get-in registry [:tc %])) topics))

(defn recv-ch [registry recv opts]
  (get-in registry [:roc recv opts]))

(def ^:private set-conj (fnil conj #{}))

(defn- set-assoc [m k v]
  (update m k set-conj v))

(defn- set-dissoc [m k v]
  (let [s (disj (get m k) v)]
    (if (seq s)
      (assoc m k s)
      (dissoc m k))))

(defn- update-topics [registry topics f & args]
  (update registry :tc
          (fn [tmap]
            (reduce (fn [m k] (apply f m k args))
                    tmap topics))))

(defn add-sub [registry {:keys [ch recv opts topics] :as sub}]
  (-> registry
      (assoc-in [:cs ch] sub)
      (assoc-in [:roc recv opts] ch)
      (update-topics topics set-assoc ch)))

(defn add-sub-topics [registry ch topics]
  (-> registry
      (update-in [:cs ch :topics] set/union topics)
      (update-topics topics set-assoc ch)))

(defn remove-sub
  ([registry ch] (remove-sub registry ch true))
  ([registry ch close?]
   (let [{:keys [recv opts topics exec-ch] :as sub} (get-in registry [:cs ch])
         opts->ch (-> (get-in registry [:roc recv])
                      (dissoc opts))
         registry (-> registry
                      (update :cs dissoc ch)
                      (update-topics topics set-dissoc ch)
                      (cond-> (and close? (some? exec-ch))
                        (assoc-in [:cs! ch] sub)))]
     (if (seq opts->ch)
       (update-in registry [:roc recv] opts->ch)
       (update registry :roc dissoc recv)))))

(defn remove-sub-topics [registry ch topics]
  (let [remain (set/difference (get-in registry [:cs ch :topics]) topics)]
    (if (seq remain)
      (-> registry
          (assoc-in [:cs ch :topics] remain)
          (update-topics topics set-dissoc ch))
      (remove-sub registry ch))))

(defn remove-all [{:keys [cs cs!]}]
  (->> (into cs! (filter #(-> % val :exec-ch)) cs)
       (assoc empty-registry :cs!)))

(defn remove-topics [registry topics]
  (reduce (fn [r ch] (remove-sub-topics r ch topics))
          registry
          (topics->chans registry topics)))

(defn remove-recv
  [registry recv]
  (->> (get-in registry [:roc recv])
       vals
       (reduce remove-sub registry)))

(defn remove-recv-topics
  [registry recv topics]
  (->> (get-in registry [:roc recv])
       vals
       (reduce (fn [r ch]
                 (remove-sub-topics r ch topics))
               registry)))

(defn removed-map [registry]
  (:cs! registry))

(defn removed-subs [registry]
  (-> registry :cs! vals))

(defn clear-removed [registry ch]
  (update registry :cs! dissoc ch))

(defn subscribe [registry recv opts topics make-sub]
  (if-let [ch (get-in registry [:roc recv opts])]
    (let [old-topics (get-in registry [:cs ch :topics])
          new-topics (set/difference topics old-topics)]
      (if (seq new-topics)
        (-> registry
            (remove-recv-topics recv new-topics)
            (add-sub-topics ch new-topics))
        registry))
    (-> registry
        (remove-recv-topics recv topics)
        (add-sub (assoc (make-sub)
                        :recv recv
                        :opts opts
                        :topics topics)))))
