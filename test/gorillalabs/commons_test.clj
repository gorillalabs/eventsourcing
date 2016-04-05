(ns gorillalabs.commons-test
  (:require
    [clojure.test :refer :all]
    [gorillalabs.commons :refer :all]))


(deftest test-parse-integer
  (is (= (parse-integer "3" nil) 3))
  (is (= (parse-integer "-3" nil) -3))
  (is (= (parse-integer "hallo" nil) nil))
  (is (= (parse-integer "hallo" 0) 0))
  (is (= (parse-integer "3.14" 0) 0))
  (is (= (parse-integer (Object.) 0) 0))
  (is (= (parse-integer nil 0) 0))
  (is (= (parse-integer nil nil) nil))
  (is (= (parse-integer 1) 1)))

(deftest test-assoc-if
  (is (= (assoc-if {} "key" "value") {"key" "value"}))
  (is (= (assoc-if {} :key :value) {:key :value}))
  (is (= (assoc-if {} :key false) {:key false}))
  (is (= (assoc-if {} "key" nil) {}))
  (is (= (assoc-if {} :key nil) {}))
  (is (= (assoc-if {} :key :value :nil nil true false) {:key :value true false}))
  )

(deftest test-assoc-if!
  (is (= (persistent! (assoc-if! (transient {}) "key" "value")) {"key" "value"}))
  (is (= (persistent! (assoc-if! (transient {}) :key :value)) {:key :value}))
  (is (= (persistent! (assoc-if! (transient {}) :key false)) {:key false}))
  (is (= (persistent! (assoc-if! (transient {}) "key" nil)) {}))
  (is (= (persistent! (assoc-if! (transient {}) :key nil)) {}))
  (is (= (persistent! (assoc-if! (transient {}) :key :value :nil nil true false)) {:key :value true false}))
  )

(deftest test-equals-ignore-case
  (is (= (equals-ignore-case "TEst" "test") true))
  (is (= (equals-ignore-case "test" "TEst") true))
  (is (= (equals-ignore-case "test" "test") true))
  (is (= (equals-ignore-case "TEST" "TEST") true))
  (is (= (equals-ignore-case "foo" "bar") false))
  (is (= (equals-ignore-case "foo" nil) false))
  (is (= (equals-ignore-case nil "bar") false))
  (is (= (equals-ignore-case nil nil) true))
  (is (= (equals-ignore-case "" "") true))
  )



(deftest test-remove-from
  (let [original {:a "b" :c "d" :e {:f "g" :h "i"}}
        to-remove {:c "d" :e {:h "1"}}]
    (is (= (remove-from original to-remove) {:a "b", :e {:f "g"}}))))

(deftest test-merge-by
  (is (= (merge-by :a [{:a 1 :b 1} {:a 2 :b 2} {:a 2 :c 3} {:a 1 :c 1}]) (list {:c 1, :a 1, :b 1} {:c 3, :a 2, :b 2}))))
