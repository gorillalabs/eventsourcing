(ns gorillalabs.eventsourcing.listener
  (:require [gorillalabs.eventsourcing.event :as event]
            [gorillalabs.commons :as commons]))

;; (def ^{:dynamic true} *event-listeners* (atom {})) ;;; FIXME: Need to get rid of this dynamic atom to allow multiple processing units.


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

(defn- parse-listener-name
  "Internal function to extract a specified listener name."
  [[arg :as all]]
  (let [[name rest] (if (symbol? arg)
                      [arg (rest all)]
                      [nil all])]
    [{::name name} rest]))

(defn- parse-bindings
  "Internal function to extract the bindings and the options."
  [[{:keys [name] :as result} [arg :as all]]]
  (when-not (vector? arg)
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

(defn parse-options
  "Internal function to parse options."
  [[{name ::name options ::options :as result} body]]
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

(defn parse-deflistener-args
  "Parses the macro signature."
  [args]
  (-> args
      (parse-listener-name)
      (parse-bindings)
      (parse-options)))


(defmacro deflistener
  "Defines an event listener to receive sourced events.

  The listener may also manipulate an aggregate. The changed aggregate will be passed to other
  listeners in a chain e.g. in the apply-events function.

  (deflistener [event :attach-to my-listeners] ...)
  (deflistener [event aggregate] ...)
  (deflistener [event :type :activated] ...)
  (deflistener my-listener [event aggregate :type [:activated]] ... )"
  [& args]
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


(defn get-listeners
  "Returns a collection of listeners suitable for handling the specified event type."
  [listeners type]
  (concat (vals (get @listeners type)) (vals (get @listeners ::all))))

(defn empty-aggregate
  "Creates a new empty aggregate for a new event. The aggregate will contain all basic meta-data.
This function should be used for create events to create an initial aggregate."
  [event]
  {:uid (event/event-uid event) :created (event/event-datetime event)})

