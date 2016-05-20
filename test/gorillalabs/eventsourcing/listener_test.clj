(ns gorillalabs.eventsourcing.listener-test
  (:require [clojure.test :refer :all]
            [gorillalabs.eventsourcing.event :as event]
            [gorillalabs.eventsourcing.listener :as listener]
            [gorillalabs.eventsourcing.core :as core]
            [juxt.iota :as i]
            )
  (:import (org.joda.time DateTime)))


(deftest test-associative-listener
  (let [associative-listeners (listener/create-listener-dictionary)]

    (listener/deflistener [event aggregate :types :assoc-test-foo :attach-to associative-listeners]
                          (assoc aggregate :foo (:foo event))
                          )

    (let [event (event/new-event :assoc-test-foo 1 {:foo "bar"})
          event (assoc event :_v 1)]
      (i/given (core/apply-events associative-listeners {} event)
               :lastModified :instanceof DateTime
               :foo := "bar"
               :version := 1
               event/aggregate-version := 1
               event/aggregate-last-modified :instanceof DateTime
               )
      )))

(deftest test-sequential-listener
  (let [sequential-listeners (listener/create-listener-dictionary)]

    (listener/deflistener [event aggregate :types :seq-test-foo :attach-to sequential-listeners]
                          (cons (:foo event) aggregate))

    (let [event (event/new-event :seq-test-foo 1 {:foo "bar"})
          event (assoc event :_v 1)]
      (i/given (core/apply-events sequential-listeners nil event)
               identity := ["bar"]
               event/aggregate-version := 1
               event/aggregate-last-modified :instanceof DateTime))))