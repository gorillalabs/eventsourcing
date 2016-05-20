(ns gorillalabs.eventsourcing.store.mongo
  (:require [clojure.tools.logging :refer :all]
            [clj-time.core :refer [now]]
            [clojure.string :refer [join]]
            [clojure.set :refer [difference]]
            [gorillalabs.commons :refer [assoc-if as-str]]
            [gorillalabs.eventsourcing.core :as core]
            [somnium.congomongo :as mg]
            [gorillalabs.eventsourcing.listener :as listener]
            [gorillalabs.eventsourcing.event :as event]
            [gorillalabs.eventsourcing.store :as store])
  ; (:import [com.mongodb BasicDBObject])
  (:import (com.mongodb BasicDBObject)))

(defn version-from*
  "Internal implementation to fetch the version from the MongoDB."
  [collection uid]
  (-> (mg/fetch collection
                :where {:uid uid :_n {:$ne :snp}}
                :sort {:_v -1}
                :only [:_v]
                :limit 1)
      first
      (get :_v 0)))

(defn load-events-from*
  "Internal implementation to load all events from the MongoDB."
  [collection uid & {:keys [from since batch-size] :or {batch-size 256}}]
  (let [query {:uid uid :_n :vnt}
        query (if from (assoc query :_v {:$gt from}) query)
        query (if since (assoc query :_d {:$gt since}) query)
        ]
    (mg/fetch collection
              :where query
              :sort {:_v 1}
              )))

(defn select-events-from* [collection query]
  "Internal implementation to select certain events from the MongoDB."
  (mg/fetch collection
            :where (assoc query :_n :vnt)
            :sort {:_v 1}))

(defn- enrich-events [current events]
  "Adds information like version and nature (event or snapshop) to the event."
  (reverse (loop [v (inc current)
                  in events
                  out nil]
             (if (empty? in)
               out
               (recur (inc v) (rest in) (cons (assoc (first in) :_v v :_n :vnt) out))))))

(defn- partition-events-by-uid
  "Partitions events by their UID."
  [r evnt]
  (if-let [uid (:uid evnt)]
    (assoc r uid (cons evnt (get r uid)))
    r))

(defn enrich-partition
  "Enriches partitions of events."
  [versions]
  (fn [r [uid events]]
    (if-let [version (get versions uid)]
      (concat r (enrich-events version (reverse events)))
      (throw (IllegalArgumentException. (str "Unable to get version for UID " uid "."))))))



(defn store-events-into* [collection versions events]
  "Internal implementation to store events into the MongoDB."
  (let [ids (reduce (fn [r e] (conj r (:uid e))) #{} events)
        versions (if (associative? versions) versions {(first ids) versions})
        version-ids (set (keys versions))]
    (if-not (empty? (difference ids version-ids))
      (throw (IllegalArgumentException. (str "You need for every aggregate a version. Missing version for: "
                                             (join \, (difference ids version-ids)))))
      (let [partitioned-events (reduce partition-events-by-uid nil events)
            enriched-events (reduce (enrich-partition versions) nil partitioned-events)]
        (try
          (mg/mass-insert! collection enriched-events)
          (catch com.mongodb.MongoException$DuplicateKey e
            (throw (IllegalStateException. (str "Optimistic locking error. Tried to insert "
                                                (join \, (map #(str "[" (:uid %) ", v" (:_v %) "]") enriched-events))
                                                " into " collection ", but at least one of them already existed.")))))))))

(defn- split-events-and-snapshot
  "Reads from a stream containing all events and snapshots and returns all events until the first snapshot
  and the snapshot."
  [stream]
  (loop [itms stream
         events nil]
    (let [itm (first itms)
          nature (keyword (:_n itm))]
      (cond
        (nil? itm) [nil events]
        (= :snp nature) [itm events]
        :else (recur (rest itms) (cons itm events))))))

(defn load-aggregate* [collection listeners uid store & {:keys [version]}]
  "Loads the aggregate for a certain ID."
  (listener/with-listeners listeners
                           (let [^BasicDBObject order (BasicDBObject.)
                             order (doto order (.append "_v" (Long. -1)) (.append "_n" (Long. 1)))
                             where {:uid uid}
                             where (if version (assoc where :_v {:$lte version}) where)
                             docs (mg/fetch collection :where where :sort order)
                             [snapshot events] (split-events-and-snapshot docs)
                             snapshot-version (:version snapshot)
                             from-snapshot (store/snapshot-to-aggregate store snapshot-version)]
                         (when-not from-snapshot (throw (IllegalStateException. (str "Unable to load snapshot for snapshot version '" snapshot-version "'."))))
                         (core/apply-events (from-snapshot (:snapshot snapshot)) events)
                         ))
  )


(defn- create-snapshot [collection aggregate store]
  "Function to create a snapshot from an aggregate in the database"
  (let [uid (event/aggregate-id aggregate)
        version (event/aggregate-version aggregate)]
    (try
      (mg/insert! collection
                  {:uid      uid
                   :_v       version
                   :_n       :snp
                   :_d       (now)
                   :version  (store/snapshot-format-version store)
                   :snapshot (store/to-snapshot-format store aggregate)}
                  :upsert? true)
      (catch com.mongodb.MongoException$DuplicateKey e
        (warn "Tried to write snapshot" version "for" uid "to" collection "but failed due to:" (.getMessage e)))
      (catch java.lang.Exception e
        (warn e "Tried to write snapshot" version "for" uid "to" collection "but failed due to:" (.getMessage e))))))

(defn create-snapshots-for* [collection listeners store {:keys [since min-delta] :or {min-delta 0}}]
  "Creates snapshots for every aggregate that seems suitable."
  (let [pipeline [{:$project {:uid 1 :_n 1 :_v 1 :event {:$cond [{:$eq [:$_n :vnt]} :$_v 0]} :snapshot {:$cond [{:$eq [:$_n :snp]} :$_v 0]}}}
                  {:$group {:_id :$uid :max-vnt {:$max :$event} :max-snp {:$max :$snapshot}}}
                  {:$project {:_id 1 :max-vnt 1 :max-snp 1 :unsnapped {:$gt [:$max-vnt :$max-snp]}}}
                  {:$match {:unsnapped true}}]
        pipeline (if since (cons {:$match {:_d {:$gte since}}} pipeline) pipeline)
        r (apply mg/aggregate collection pipeline)]
    (listener/with-listeners listeners
                         (doseq [c (:result r)
                                 :let [uid (:_id c)
                                       max-snp (get c :max-snp 0)
                                       max-vnt (get c :max-vnt 0)]
                                 :when (and uid (> (max 0 (- max-vnt min-delta)) max-snp))
                                 ]
                           (let [events (load-events-from* collection uid)
                                 aggregate (core/apply-events (listener/empty-aggregate (first events)) events)]
                             (create-snapshot collection aggregate store)
                             )))))


(deftype MongoStore [collection database-fn listeners snapshot-version to-snapshot-fn from-snapshot-fn-map]
  store/EventStore
  (load-events-from [_ uid options]
    (mg/with-mongo (database-fn)
                   (when uid (apply load-events-from* collection uid options))))
  (load-aggregate-from [this uid options]
    (mg/with-mongo (database-fn)
                   (when uid (apply load-aggregate* collection listeners uid this options))))
  (select-events-from [_ query]
    (mg/with-mongo (database-fn)
                   (when query (listener/with-listeners listeners (select-events-from* collection query)))))
  (version-from [_ uid]
    (mg/with-mongo (database-fn) (when uid (version-from* collection uid))))
  (store-events-into [_ version events]
    (mg/with-mongo (database-fn) (store-events-into* collection version events)))
  (create-snapshots-for [this options]
    (mg/with-mongo (database-fn)
                   (create-snapshots-for* collection listeners this options)))
  (to-snapshot-format [_ aggregate] (to-snapshot-fn aggregate))
  (snapshot-format-version [_] snapshot-version)
  (snapshot-to-aggregate [_ version] (or (get from-snapshot-fn-map version) (get from-snapshot-fn-map nil)))


  java.lang.Object
  (toString [this] (str "<#MongoStore: " collection ">")))

(defn create-store [collection database-fn listeners & {:keys [snapshot-version to-snapshot from-snapshot]
                                                        :or   {snapshot-version 0 to-snapshot identity from-snapshot {nil identity}}}]
  (MongoStore. collection database-fn listeners snapshot-version to-snapshot from-snapshot))

(defn ensure-index [^MongoStore store]
  (when-let [collection (.collection store)]
    (mg/set-collection-write-concern! collection :acknowledged)
    (mg/add-index! collection [:uid :_n :_v] :name (str (name collection) "-events-main-index") :unique true)))
