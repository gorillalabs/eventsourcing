(ns gorillalabs.eventsourcing.store.memory-test
  (:require [clojure.test :refer :all]
            [juxt.iota :as i]
            [gorillalabs.eventsourcing.core :refer :all]
            [gorillalabs.eventsourcing.event :refer :all]
            [gorillalabs.eventsourcing.listener :refer :all]
            [gorillalabs.eventsourcing.store :refer :all]
            [gorillalabs.eventsourcing.store.memory :as mem]
            ))

(deftest test-memory-store
  (let [store (mem/create-store)
        id (create-id store)
        listeners (create-listener-dictionary)]
    (with-listeners listeners
                    (deflistener [event root :types :test :attach-to listeners]
                                 (assoc root :foo (:foo event))
                                 )

                    (i/given (load-events store id)
                             identity := nil)

                    (store-events store 0
                                  (list
                                    (new-event :test id {:foo "baz"})
                                    (new-event :test id {:foo "bar"})))

                    (i/given (load-events store id)
                             second :⊃ {:_v 2 :foo "bar"})

                    (i/given (load-aggregate store id)
                             identity :⊃ {:version 2 :foo "bar"}))))

