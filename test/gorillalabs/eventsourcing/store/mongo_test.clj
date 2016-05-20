(ns gorillalabs.eventsourcing.store.mongo-test
  (:require [clojure.test :refer :all]
            [juxt.iota :as i]
            [gorillalabs.eventsourcing.listener :as listeners]
            [gorillalabs.eventsourcing.store.mongo :as mongo]
            [clj-time.core :as time :refer [now]]
            [somnium.congomongo :as mg]
            [clj-time.coerce :as tc]
            [gorillalabs.eventsourcing.event :as event])
  (:import (java.util UUID)))


(let [create-id (fn [] (tc/to-long (time/now)))
      uid (UUID/randomUUID)
      listeners (listeners/create-listener-dictionary)
      test-events [{:_id (create-id), :_v 1, :_t :created, :_d (time/now), :uid uid, :locale java.util.Locale/GERMANY, :_n :vnt}
                   {:_id (create-id), :_v 2, :_t :device-added, :_d (time/now), :uid uid, :device {:locale "de_DE", :deviceId (str (create-id))}, :_n :vnt}
                   {:_id (create-id), :_v 3, :_t :activated, :_d (time/now), :uid uid, :_n :vnt}]]

  (listeners/deflistener [event aggregate :types :deactivated :attach-to listeners]
                         (assoc aggregate :status "deactivated"))
  (listeners/deflistener [event aggregate :types :activated :attach-to listeners]
                         (assoc aggregate :status "activated"))


  (deftest test-load-events-from*
    (with-redefs-fn
      {#'mg/fetch (fn [& args]
                    (i/given args
                             first := :test
                             rest := [:where {:uid uid :_n :vnt}
                                      :sort {:_v 1}])
                    test-events)}

      #(i/given (mongo/load-events-from* :test uid)
                [0] :⊃ {:_v 1 :_t :created}
                [1] :⊃ {:_v 2 :_t :device-added}
                [2] :⊃ {:_v 3 :_t :activated}
                )))


  (deftest test-version-from*
    (with-redefs-fn
      {#'mg/fetch (fn [coll & {:as opts}]
                    ;; test args
                    (is (= coll :test))
                    (i/given opts
                             :where := {:uid uid :_n {:$ne :snp}} ;; make sure it's ok to use :$ne and/or "$ne"
                             :sort := {:_v -1}
                             :only := [:_v]
                             :limit := 1)
                    ;; mock this return value
                    (reverse test-events))}
      #(is (= (mongo/version-from* :test uid) 3))))


  (deftest select-events-from*
    (with-redefs-fn
      {#'mg/fetch (fn [coll & {:as opts}]
                    ;; test args
                    (is (= coll :test))
                    (i/given opts
                             :where := {:_t :device-added :_n :vnt}
                             :sort := {:_v 1})
                    ;; mock this return value
                    (get test-events 2))}
      #(i/given (mongo/select-events-from* :test {:_t :device-added})
                identity := (get test-events 2))))

  (deftest test-store-events-into*
    (with-redefs-fn
      {#'mg/insert!          (fn [coll obj & {:as opts}]
                               ;; test args
                               (is (= coll :test))
                               (i/given obj
                                        [0 :_v] := 4
                                        [0 :uid] := uid
                                        ; [0 :_d] := anything
                                        [0 :_t] := :deactivated
                                        [0 :_n] := :vnt
                                        [0 :reason] := "Just 4 fun.")
                               (i/given opts
                                        :many := true)
                               ;; mock this return value
                               (list {:_v     4 :uid uid :_d (now) :_t :deactivated :_n :vnt
                                      :reason "Just 4 fun."}))

       #'mongo/version-from* (fn [coll id]
                               (is (= coll :test))
                               (is (= id uid))
                               3)}
      #(i/given (listeners/with-listeners listeners
                                          (mongo/store-events-into* :test 3 (list (event/new-event :deactivated uid {:reason "Just 4 fun."}))))
                [0 :_v] := 4
                [0 :uid] := uid
                ; [0 :_d] := anything
                [0 :_t] := :deactivated
                [0 :_n] := :vnt
                [0 :reason] := "Just 4 fun."
                )))

  (let [[c d a] test-events
        snaphot {:_id (UUID/randomUUID), :_v 2, :_d (now), :uid uid, :_n :snp :snapshot {:locale java.util.Locale/GERMANY, :device {:deviceId (str (UUID/randomUUID))}}}
        with-snapshots (list c d snaphot a)
        store (mongo/create-store :test identity listeners)
        ]
    (deftest test-load-aggregate*
      (with-redefs-fn
        {#'mg/fetch (fn [coll & {:as opts}]
                      ;; test args
                      (is (= coll :test))
                      (i/given opts
                               :where := {:uid uid}
                               identity :? #(contains? % :sort))
                      ;; mock this return value
                      (reverse with-snapshots))}
        #(i/given (mongo/load-aggregate* :test listeners uid store)
                  keys :? (fn [k] (= (into #{} k)
                                     #{:status :locale :version :device :lastModified}))
                  :status := "activated"
                  :locale := java.util.Locale/GERMANY
                  :version := 3
                  :device :? (fn [i] (contains? i :deviceId))))))

  (let [[c d a] test-events
        snaphot {:_id (UUID/randomUUID), :_v 2, :_d (now), :uid uid, :_n :snp :snapshot {:locale java.util.Locale/GERMANY, :device {:deviceId (str (UUID/randomUUID))}}}
        with-snapshots (list c d snaphot a)
        store (mongo/create-store :test identity listeners)
        ]
    (deftest test-load-aggregate*-with-version
      (with-redefs-fn
        {#'mg/fetch (fn [coll & {:as opts}]
                      ;; test args
                      (is (= coll :test))
                      (i/given opts
                               :where := {:uid uid :_v {:$lte 3}}
                               identity :? #(contains? % :sort))
                      ;; mock this return value
                      (reverse with-snapshots))}
        #(i/given (mongo/load-aggregate* :test listeners uid store :version 3)
                  keys :? (fn [k] (= (into #{} k)
                                     #{:status :locale :version :device :lastModified}))
                  :status := "activated"
                  :locale := java.util.Locale/GERMANY
                  :version := 3
                  :device :? (fn [i] (contains? i :deviceId)))))))
