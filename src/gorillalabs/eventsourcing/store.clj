(ns gorillalabs.eventsourcing.store)

(defprotocol EventStore
  "Protocol that defines an event store, to store and load events from."
  (create-id [this] "Creates an event ID.")
  (load-events-from [this uid options] "Loads all event for a specified aggregate ID (uid).")
  (load-aggregate-from [this uid options] "Loads an aggregate for a specified ID (uid).")
  (select-events-from [this query] "Loads all event for a specified aggregate ID (uid) that match the specified query.")
  (version-from [this uid] "Returns the current version for an aggregate")
  (store-events-into [this version events] "Stores all events ")
  (create-snapshots-for [this options] "Creates snapshots for all aggreates that may apprear suitable.")
  (to-snapshot-format [this aggregate] "Converts the aggregate into the snapshot format used by this store.")
  (snapshot-format-version [this] "Returns a version identifier to identify the snapshot format version currently in use.")
  (snapshot-to-aggregate [this version] "Returns a conversion function to create an aggregate from the snapshot."))