(ns gorillalabs.eventsourcing.memory
  (:use clojure.tools.logging
        [gorillalabs.eventsourcing.core :as core]
        [gorillalabs.commons :only (assoc-if as-str)])
  (:import (java.util UUID)))

(deftype MemoryStore [store]
  core/EventStore
  (create-id [_]
    (UUID/randomUUID))
  (load-events-from [_ uid options]
    (when uid
      (when-let [vals (filter #(= (event-uid %1) uid) @store)]
        (when-not (empty? vals) vals))))
  (load-aggregate-from [this uid options]
    (when uid
      (apply-events nil (core/load-events-from this uid nil))))
  (select-events-from [_ query] nil)
  (version-from [this uid]
    (when uid
      (let [v (event-version (first (reverse (core/load-events-from this uid nil))))]
        (if v v 0))))
  (store-events-into [this version events]
    (let [ids (reduce (fn [r e] (conj r (:uid e))) #{} events)
          current (core/version-from this (first ids))]
      (cond
        (nil? version) (throw (IllegalArgumentException. "Version number must not be nil."))
        (not (number? version)) (throw (IllegalArgumentException. (str "Version number has to be numeric, but was '" version "'.")))
        (> (count ids) 1) (throw (IllegalArgumentException. (str "All events have to refer to the same aggregate UID (got " ids ")")))
        (not= version current) (throw (IllegalStateException. (str "Expected version " version " but current version is " current ".")))
        :else (let [result (loop [v (inc current), in events, out (sorted-set-by #(compare (event-version %1) (event-version %2)))]
                             (if (empty? in)
                               out
                               (recur (inc v) (rest in) (conj out (-> (first in)
                                                                      (assoc :_v v)
                                                                      (assoc :_id (core/create-id this)))))))]
                (swap! store (fn change-store [curr] (concat curr result)))
                (apply list result)
                ))))
  (create-snapshots-for [this options]
    nil                                                     ; Snapshots are not supported.
    )
  (to-snapshot-format [_ aggregate] aggregate)
  (snapshot-format-version [_] 0)
  (snapshot-to-aggregate [_ _] identity)
  java.lang.Object
  (toString [_] "<#MemoryStore>")
  )


(defn create-store
  "Creates a new emtpy store"
  ([] (MemoryStore. (atom '())))
  ([events] (MemoryStore. (atom (if (sequential? events) (apply list events) (list events)))))
  ([event & events] (create-store (cons event events))))

(defn all-events
  ([^MemoryStore store] @(.store store))
  ([^MemoryStore store uid]
   (filter #(= uid (:uid %1)) @(.store store))))