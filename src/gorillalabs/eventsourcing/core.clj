(ns gorillalabs.eventsourcing.core
  (:require
    [gorillalabs.eventsourcing.store :as store]
    [gorillalabs.eventsourcing.event :as event]
    [gorillalabs.eventsourcing.listener :as listener]
    [clojure.tools.logging :refer :all]
    [clj-time.core :as time]
    [gorillalabs.commons :as commons]))




(def ^{:dynamic true} *asynchronous-publishing* false)


(defn apply-events
  "Uses an event stream to perform a projection on an aggregate root."
  ([listeners aggregate events]
   (if (empty? events)
     aggregate
     (reduce (fn [root event]
               (let [projection (reduce #(%2 event %1) root (listener/get-listeners listeners (event/event-type event)))]
                 (if (associative? projection)
                   (-> projection
                       (commons/assoc-if :version (event/event-version event))
                       (commons/assoc-if :lastModified (event/event-datetime event)))
                   (when projection
                     (with-meta projection
                                (commons/assoc-if {} :version (event/event-version event) :lastModified (event/event-datetime event)))))))
             aggregate (if (sequential? events) events (list events)))))
  ([listeners aggregate event & events] (apply-events listeners aggregate (cons event events))))

(defn current-version
  "Reads the current version for an aggregate."
  [store uid]
  (store/version-from store uid))


(defn store-events
  "Stores events for a dedicated aggregate into the given store. All events specified have to refer to
the same aggregate (ID)."
  ([store version events]
   (let [eventList (if (seq? events) events (list events))]
     (store/store-events-into store version eventList)))
  ([collection version event & events]
   (store-events collection version (cons event events))))

(def ^:private event-publisher (agent nil :error-handler (fn [agent error] (warn error "An unexpected error occured while publishsing events."))))

(defn- perform-publishing-fn [event dest]
  (fn [_] (doseq [listener dest] (listener event))))

(defn- publish-directly [event dest]
  (doseq [listener dest]
    (try
      (listener event)
      (catch java.lang.Exception e
        (warn e "An unexpected error occured while publishsing events.")))))

(defn execute-publishing!
  "This function does perform the publishing it self."
  [event dest]
  (if *asynchronous-publishing*
    (send-off event-publisher (perform-publishing-fn event dest)) ;call publishing fn via agent
    (publish-directly event dest)                           ;call publishing fn directly
    ))

(defn publish-events!
  "This function publishes the event to a specified set of listeners. This will happen asynchronously. The order
of the events will be preserved but there is no guarantee about the order of notification.

Note: At the moment there is no strong guarantee about the delivery of the events. So ig i.e. the system shuts down
events may get lost."
  [listeners events]
  (doseq [event events]
    (let [type (event/event-type event)
          dest (listener/get-listeners listeners type)]
      (execute-publishing! event dest)))
  events)

(defn load-events
  "Loads events for a aggregate from the specified event store."
  [store uid & options]
  (store/load-events-from store uid options))

(defn select-events
  "Loads certain events for an aggregate defined by the query from the specified event store."
  [store query]
  (store/select-events-from store query))

(defn load-aggregate
  "Loads an aggregate from the specified event store."
  [store uid & options]
  (store/load-aggregate-from store uid options))


(defn create-snapshots
  "Creates snapshots in the store to optimize loading if the aggregates."
  [store & options]
  (store/create-snapshots-for store options))