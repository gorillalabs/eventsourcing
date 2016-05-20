(ns gorillalabs.eventsourcing.event
  (:require [clj-time.core :as time]))



(defn new-event
  "Creates a new event for the specified type, aggregate id and payload."
  [type id payload]
  (let [now (time/now)]
    ;; this was to warn if we have a typo in the event types
    #_(when (empty? (get-listeners *event-listeners* type))
      (warn (str "No projection available for event with type '" type "'.")))
    (-> (merge payload {:uid id :_d now :_t type}))))


(defn event-type
  "Accessor function for type of an event."
  [event]
  (keyword (get event :_t)))

(defn event-datetime
  "Accessor function for the datetime of an event."
  [event]
  (get event :_d))

(defn event-uid
  "Accessor function for aggregate ID (uid) of an event."
  [event]
  (get event :uid))

(defn event-version
  "Accessor function for the aggreget version that an events represents.."
  [event]
  (get event :_v))


(defn aggregate-id
  "Accessor function for the aggregate ID."
  ([aggregate]
   (get aggregate :uid))
  #_([aggregate v]
      (assoc aggregate :uid (db/create-id v))))

(def id aggregate-id)

(defn aggregate-version
  "Accessor function to the aggregate version."
  [aggregate]
  (let [meta (meta aggregate)
        meta-version (:version meta)
        assocative-version (:version aggregate)]
    (or meta-version assocative-version)))

(def version aggregate-version)

(defn aggregate-last-modified
  "Accessor function for the aggregate last modifed timestamp."
  [aggregate]
  (let [meta (meta aggregate)
        meta-last-modified (:lastModified meta)
        assocative-last-modified (:lastModified aggregate)]
    (or meta-last-modified assocative-last-modified)))

(def last-modified aggregate-last-modified)