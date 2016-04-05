(ns gorillalabs.eventsourcing.core
  (:require [clojure.tools.logging :refer :all]
            [clj-time.core :as time]
            [gorillalabs.commons :as commons]))


(def ^{:private true :dynamic true} *event-listeners* (atom {}))

(def ^{:dynamic true} *asynchronous-publishing* false)

(defprotocol EventStore
  "Protocol that defines an event store, to store and load events from"
  (create-id [this] "Creates an event ID")
  (load-events-from [this uid options] "Loads all event for a specified aggregate ID (uid)")
  (load-aggregate-from [this uid options] "Loads an aggregate for a specified ID (uid)")
  (select-events-from [this query] "Loads all event for a specified aggregate ID (uid) that match the specified query.")
  (version-from [this uid] "Returns the current version for an aggregate")
  (store-events-into [this version events] "Stores all events ")
  (create-snapshots-for [this options] "Creates snapshots for all aggreates that may apprear suitable.")
  (to-snapshot-format [this aggregate] "Converts the aggregate into the snapshot format used by this store.")
  (snapshot-format-version [this] "Returns a version identifier to identify the snapshot format version currently in use.")
  (snapshot-to-aggregate [this version] "Returns a conversion function to create an aggregate from the snapshot.")
  )


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


(defn event-type
  "Accessor function for type of an event."
  [event]
  (keyword (get event :_t)))

(defn event-date
  "Accessor function for the date of an event."
  [event]
  (get event :_d))

(defn event-uid [event]
  "Accessor function for aggregate ID (uid) of an event."
  (get event :uid))

(defn event-version [event]
  "Accessor function for the aggreget version that an events represents.."
  (get event :_v))

(defn event-remote-address [event]
  "Accessor function for the remote IP an event was initiated from."
  (get event :_ip))

(defn event-admin [event]
  "Accessor function for the admin ID who initiated that event."
  (get event :_adm))


(defn create-listener-dictionary
  "creates a atomic dictionary of listeners. You'll may specify e.g. :unique to make sure that no duplicate listeners
  for any event will be accepted."
  ([] (create-listener-dictionary false))
  ([unique] (atom (with-meta {} {:unique (or unique (= :unique unique))}))))

(defn attach-to
  "Attaches an event listener to one or more listener dictionaries."
  ([listener listeners]
   (let [unique (:unique (meta @listeners))
         listener-meta (meta listener)
         types (if-let [ts (::types listener-meta)] ts '(::all))
         name (::name listener-meta)]
     (swap! listeners
            (fn [map] (reduce
                        (fn [v type]
                          (if (and unique (pos? (count (get v type))) (not (get-in v [type name])))
                            (throw (IllegalStateException. (str "Listener for event type '" type "' already exists.")))
                            (assoc-in v [type name] listener)
                            )) map types)))))
  ([listener listeners & other]
   (doseq [l (cons listeners other)]
     (attach-to listener l))))

(defn- parse-listener-name [[arg :as all]]
  "Internal function to extract a specified listener name."
  (let [[name rest] (if (symbol? arg)
                      [arg (rest all)]
                      [nil all])]
    [{::name name} rest]))

(defn- parse-bindings [[{:keys [name] :as result} [arg :as all]]]
  "Internal function to extract the bindings and the options."
  (when-not (vector? arg) ()
                          (throw (IllegalArgumentException. (str "Bindings must be a vector, not a " (class arg) "."))))
  (let [[event-bind & other] arg
        [root-bind options] (if (keyword? (first other))
                              [nil other]
                              [(first other) (rest other)])
        updated (assoc result
                  ::event-bind event-bind
                  ::aggregate-bind root-bind
                  ::options (apply hash-map options))]
    [updated (rest all)]))

(defn parse-options [[{name ::name options ::options :as result} body]]
  "Internal function to parse options."
  (let [types (:types options)
        attach (:attach-to options)
        type-coll (cond (coll? types) types (nil? types) [::all] :else [types])
        attach-coll (cond (coll? attach) attach (nil? attach) nil :else [attach])]
    (-> result
        (assoc ::name (or name (if (empty? type-coll) 'all-events-listener (symbol (apply str (conj (interpose "-and-" (map commons/as-str type-coll)) "listener-for-"))))))
        (commons/assoc-if ::types type-coll)
        (commons/assoc-if ::attach attach-coll)
        (assoc ::body body)
        )
    ))

(defn parse-deflistener-args [args]
  "Parses the macro signature."
  (-> args
      (parse-listener-name)
      (parse-bindings)
      (parse-options)))


(defmacro deflistener [& args]
  "Defines an event listener to receive souced events.

  The listener may also manipulate an aggregate. The changed aggregate will be passed to other
  listeners in a chain e.g. in the apply-events function.

  (deflistener [event :attach-to my-listeners] ...)
  (deflistener [event aggregate] ...)
  (deflistener [event :type :activated] ...)
  (deflistener my-listener [event aggregate :type [:activated]] ... )
  "
  (let [{listener-name# ::name event-bind# ::event-bind root-bind# ::aggregate-bind
         types#         ::types attach# ::attach body# ::body} (parse-deflistener-args args)
        attach-body# (when attach# `((attach-to ~listener-name# ~@attach#)))
        [with-root-body# aggregate-name#] (if root-bind# [body# root-bind#]
                                                         (let [vnme (gensym "__aggregate_")] [`((do ~@body# ~vnme)) vnme]))
        ]
    `(do (def ~listener-name# (with-meta (fn ~listener-name#
                                           ([~event-bind#] (~listener-name# ~event-bind# nil))
                                           ([~event-bind# ~aggregate-name#] ~@with-root-body#)
                                           ) {::types ~types# ::name ~(keyword (str *ns*) (commons/as-str listener-name#))}))
         ~@attach-body#)))


(defn get-listeners [listeners type]
  "Returns a collection of listeners suitable for handling the specified event type."
  (concat (vals (get @listeners type)) (vals (get @listeners ::all))))

(defn empty-aggregate [event]
  "Creates a new empty aggregate for a new event. The aggregate will contain all basic meta-data.
This function should be used for create events to create an initial aggregate."
  {:uid (event-uid event) :created (event-date event)})

(defn new-event [type id payload & {:keys [remote-address admin]}]
  "Creates a new event for the specified type, aggregate id and payload."
  (let [now (time/now)
        mutator (get-listeners *event-listeners* type)]
    (when (empty? mutator)
      (warn (str "No projection available for event with type '" type "'.")))
    (-> (merge payload {:uid id :_d now :_t type})
        (commons/assoc-if :_ip remote-address :_adm admin))))

(defmacro with-listeners [listeners & body]
  "Defines the event listeners for the given body."
  `(binding [*event-listeners* ~listeners]
     ~@body))

(defn apply-events
  "Uses an event stream to perform a projection on an aggregate root."
  ([aggregate events]
   (if (empty? events)
     aggregate
     (reduce (fn [root event]
               (let [projection (reduce #(%2 event %1) root (get-listeners *event-listeners* (event-type event)))]
                 (if (associative? projection)
                   (-> projection
                       (commons/assoc-if :version (event-version event))
                       (commons/assoc-if :lastModified (event-date event)))
                   (when projection
                     (with-meta projection
                                (commons/assoc-if {} :version (event-version event) :lastModified (event-date event)))))))
             aggregate (if (sequential? events) events (list events)))))
  ([aggregate event & events] (apply-events aggregate (cons event events))))

(defn current-version [store uid]
  "Reads the current version for an aggregate."
  (version-from store uid))


(defn store-events
  "Stores events for a dedicated aggregate into the database. All events specified have to refer to
the same aggregate (ID)."
  ([store version events]
   (let [eventList (if (seq? events) events (list events))]
     (store-events-into store version eventList)))
  ([collection version event & events] (store-events version (cons event events))))

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
  "This funktion does perform the publishing it self."
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
  ([events]
   (publish-events! *event-listeners* events))
  ([listeners events]
   (doseq [event events]
     (let [type (event-type event)
           dest (get-listeners listeners type)]
       (execute-publishing! event dest)))
   events))

(defn load-events [store uid & options]
  "Loads events for a aggregate from the specified event store."
  (load-events-from store uid options))

(defn select-events [store query]
  "Loads certain events for an aggregate defined by the query from the specified event store."
  (select-events-from store query))

(defn load-aggregate [store uid & options]
  "Loads an aggregate from the specified event store."
  (load-aggregate-from store uid options))


(defn create-snapshots [store & options]
  "Creates snapshots in the store to optimize loading if the aggregates."
  (create-snapshots-for store options))