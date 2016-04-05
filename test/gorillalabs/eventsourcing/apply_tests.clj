(ns gorillalabs.eventsourcing.apply-tests
  (:require [clojure.test :refer :all]
            [gorillalabs.eventsourcing.core :refer :all]
            [juxt.iota :as i]
            )
  (:import (org.joda.time DateTime)))


(deftest test-associative-listener
  (def associative-listeners (create-listener-dictionary))

  (deflistener [event aggregate :types :assoc-test-foo :attach-to associative-listeners]
               (assoc aggregate :foo (:foo event))
               )

  (with-listeners associative-listeners
                  (let [event (new-event :assoc-test-foo 1 {:foo "bar"})
                        event (assoc event :_v 1)]
                    (i/given (apply-events {} event)
                             :lastModified :instanceof DateTime
                             :foo := "bar"
                             :version := 1
                             aggregate-version := 1
                             aggregate-last-modified :instanceof DateTime
                             )
                    )))

(deftest test-sequential-listener
  (def sequential-listeners (create-listener-dictionary))

  (deflistener [event aggregate :types :seq-test-foo :attach-to sequential-listeners]
               (cons (:foo event) aggregate)
               )

  (with-listeners sequential-listeners
                  (let [event (new-event :seq-test-foo 1 {:foo "bar"})
                        event (assoc event :_v 1)]
                    (i/given (apply-events nil event)
                             identity := ["bar"]
                             aggregate-version := 1
                             aggregate-last-modified :instanceof DateTime
                             )
                    )))