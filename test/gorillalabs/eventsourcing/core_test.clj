(ns gorillalabs.eventsourcing.core-test
  (:require [clojure.test :refer :all]
            [juxt.iota :as i]
            [gorillalabs.eventsourcing.core :refer :all]
            [clj-time.core :as time :only (now)]))

(let [modified (time/now)
      aggregate {:version 1 :lastModified modified :foo "bar"}]
  (deftest test-associative-aggregates
    (i/given aggregate
             aggregate-version := 1
             aggregate-last-modified := modified))
  )

(let [modified (time/now)
      aggregate (with-meta '("foo") {:version 1 :lastModified modified})]
  (deftest test-other-aggregates
    (i/given aggregate
             aggregate-version := 1
             aggregate-last-modified := modified)))
