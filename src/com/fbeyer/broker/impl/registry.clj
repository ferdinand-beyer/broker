(ns com.fbeyer.broker.impl.registry
  {:skip-wiki true}
  (:require [clojure.set :as set]))

(def empty-registry
  {:cs  {} ; ch->sub
   :roc {} ; recp->opts->ch
   :tc  {} ; topic->ch
   :cs! {} ; ch->sub (removed)
   })

(defn- get-sub [registry ch]
  (get-in registry [:cs ch]))

(defn all-subs [registry]
  (-> registry :cs vals))

(defn topics->chans [registry topics]
  (into #{} (mapcat #(get-in registry [:tc %])) topics))

(defn recp-subs [registry recp]
  (->> (get-in registry [:roc recp])
       vals
       (map (partial get-sub registry))))

(defn recp-topics [registry recp]
  (->> (recp-subs registry recp)
       (mapcat :topics)
       (into #{})))

(defn sub-by-recp-opts [registry recp opts]
  (when-let [ch (get-in registry [:roc recp opts])]
    (get-sub registry ch)))

(defn topic-subs [registry topic]
  (->> (get-in registry [:tc topic])
       (map (partial get-sub registry))))

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

(defn add-sub [registry {:keys [ch recp opts topics] :as sub}]
  (-> registry
      (assoc-in [:cs ch] sub)
      (assoc-in [:roc recp opts] ch)
      (update-topics topics set-assoc ch)))

(defn add-sub-topics [registry ch topics]
  (-> registry
      (update-in [:cs ch :topics] set/union topics)
      (update-topics topics set-assoc ch)))

(defn remove-sub [registry ch]
  (let [{:keys [recp opts topics exec-ch] :as sub} (get-in registry [:cs ch])
        oc       (-> (get-in registry [:roc recp])
                     (dissoc opts))
        registry (-> registry
                     (update :cs dissoc ch)
                     (update-topics topics set-dissoc ch)
                     (cond-> (some? exec-ch) (assoc-in [:cs! ch] sub)))]
    (if (seq oc)
      (update-in registry [:roc recp] oc)
      (update registry :roc dissoc recp))))

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

(defn remove-recp
  [registry recp]
  (->> (get-in registry [:roc recp])
       vals
       (reduce remove-sub registry)))

(defn remove-recp-topics
  [registry recp topics]
  (->> (get-in registry [:roc recp])
       vals
       (reduce (fn [r ch]
                 (remove-sub-topics r ch topics))
               registry)))

(defn removed-subs [registry]
  (-> registry :cs! vals))

(defn clear-removed [registry ch]
  (update registry :cs! dissoc ch))

(defn subscribe [registry recp opts topics make-sub]
  (if-let [ch (get-in registry [:roc recp opts])]
    (let [old-topics (get-in registry [:cs ch :topics])
          new-topics (set/difference topics old-topics)]
      (if (seq new-topics)
        (-> registry
            (remove-recp-topics recp new-topics)
            (add-sub-topics ch new-topics))
        registry))
    (-> registry
        (remove-recp-topics recp topics)
        (add-sub (assoc (make-sub)
                        :recp recp
                        :opts opts
                        :topics topics)))))
