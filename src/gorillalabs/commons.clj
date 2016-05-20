(ns gorillalabs.commons
  (:require [clojure.tools.logging :as log]))

(defn parse-integer
  "Untility function to coerce a value from a string to an integer. If an error occurs the optional
  default value will be returned. If nil is supplied as n also the default is returned"
  ([^String n] (parse-integer n nil))
  ([^String n def]
   (cond
     (nil? n) def
     (number? n) (int n)
     (string? n) (try (Integer/parseInt n)
                      (catch Exception e
                        def))
     :else def)))

(defn round
  "Rounds a double to the specified number of fraction digits. Called with only one argument it will return a
  rounding function which will perform the rounding to the specified fraction digits."
  ([fraction]
   (let [f (Math/pow 10.0 fraction)]
     (fn [d] (when d
               (/ (Math/round (* d f)) f)))))
  ([d fraction]
   (when d
     (let [f (Math/pow 10.0 fraction)]
       (/ (Math/round (* d f)) f)))))

(defn assoc-if
  "conditional assoc[iate]. When applied with a not nil value to a map, returns a new map of the
same (hashed/sorted) type, that contains the mapping of key(s) to val(s). When applied to a vector,
returns a new vector that contains val at index, if the val is not nil.
Note - index must be <= (count vector)."
  ([map key value]
   (if (nil? value)
     map
     (assoc map key value)))
  ([map key val & kvs]
   (let [ret (assoc-if map key val)]
     (if kvs
       (recur ret (first kvs) (second kvs) (nnext kvs))
       ret))))

(defn assoc-if!
  "conditional assoc[iate] on transient data structures . When applied with a not nil value to a map,
returns a new map of the same (hashed/sorted) type, that contains the mapping of key(s) to val(s).
When applied to a vector, returns a new vector that contains val at index, if the val is not nil.
Note - index must be <= (count vector)."
  ([map key value]
   (if (nil? value)
     map
     (assoc! map key value)))
  ([map key val & kvs]
   (let [ret (assoc-if! map key val)]
     (if kvs
       (recur ret (first kvs) (second kvs) (nnext kvs))
       ret)))
  )

(defn as-str
  ([] "")
  ([x] (if (instance? clojure.lang.Named x)
         (name x)
         (str x)))
  ([x & ys]
   ((fn [^StringBuilder sb more]
      (if more
        (recur (.append sb (as-str (first more))) (next more))
        (str sb)))
     (new StringBuilder ^String (as-str x)) ys)))

(defn warn-if-nil [x msg]
  (when (nil? x)
    (log/warn msg))
  x)


(defn dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
nested structure. keys is a sequence of keys. Any empty maps that result
will not be present in the new structure."
  ; from: http://www.clodoc.org/doc/clojure.contrib.core/dissoc-in
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))


(defn remove-nil
  ([map]
   (persistent! (reduce (fn [map-without-nils [key val]] (if (nil? val) map-without-nils (assoc! map-without-nils key val))) (transient {}) map)))
  ([map keys]
   (if (nil? (get-in map keys))
     (dissoc-in map keys)
     map
     ))
  ([map keys & more-keys]
   (apply remove-nil (remove-nil map keys) more-keys)))


(defn nilify
  "Retuns the collection, if it is not empty, returns nil otherwise. (Nil-ifys an empty collection)"
  [coll]
  (when-not (empty? coll) coll))


; taken from map-utils in 1.2 contrib library
(defn deep-merge-with
  "Like merge-with, but merges maps recursively, applying the given fn
only when there's a non-map at a particular level.

(deepmerge + {:a {:b {:c 1 :d {:x 1 :y 2}} :e 3} :f 4}
           {:a {:b {:c 2 :d {:z 9} :z 3} :e 100}})
-> {:a {:b {:z 3, :c 3, :d {:z 9, :x 1, :y 2}}, :e 103}, :f 4}"
  [f & maps]
  (apply
    (fn m [& maps]
      (if (every? map? maps)
        (apply merge-with m maps)
        (apply f maps)))
    maps))

(defn remove-from
  "Inverted merge. Removes recursively all values contained in the remove map from the original map."
  [m remove]
  (cond
    (empty? remove) m
    (empty? m) m
    (= m remove) nil
    :else (reduce (fn [m [k v]]
                    (if-let [pm (get m k)]
                      (if (associative? pm)
                        (let [new (remove-from pm v)]
                          (if (empty? new)
                            (dissoc m k)
                            (assoc m k new)
                            )
                          )
                        (dissoc m k)
                        )
                      m
                      )
                    ) m remove)
    )
  )

(defn equals-ignore-case
  "Compares two strings ignoring the case"
  [^String a ^String b]
  (if (nil? a)
    (nil? b)
    (.equalsIgnoreCase a b)
    )
  )

;; TODO: Make these into a test
;; (herolabs.commons/hash-by :a [{:a 1 :b 1} {:a 2 :b 2}])
;; -> {1 {:a 1, :b 1}, 2 {:a 2, :b 2}}

;; (herolabs.commons/hash-by [:a :b] [{:a 1 :b 1 :c 1} {:a 2 :b 2 :c 2}])
;; -> {1 {:a 1, :b 1}, 2 {:a 2, :b 2}}


(defn create-key
  "'Reduces' the entry (a map) to a key. If 'key' is an atom, the returned key is the value of that key in the map.
  If key is a sequence, the returned key is a map (and equals to select-keys.) For use in hash-by and related functions."
  [entry key]
  (if (sequential? key)
    (select-keys entry key)
    (get entry key)))

(defn hash-by
  "Maps a collection of maps by a specific key. E.g. a collection of matches (maps) by :uid, see clojure.set/index"
  [key collection]
  (persistent! (reduce (fn [m entry]
                         (assoc! m (create-key entry key) entry)) (transient {}) collection)))

;; (merge-by :a [{:a 1 :b 1} {:a 2 :b 2} {:a 2 :c 3} {:a 1 :c 1}])
;; --> ({:a 1, :c 1, :b 1} {:a 2, :c 3, :b 2})
(defn merge-by
  "Merges a collection coll of maps where the maps share a common key k."
  [k coll]
  (map #(apply merge %) (vals (group-by k coll))))

(defn ^String escape-by-fn
  "Returns a new string, using the test function to check if a char needs to be replaced and the replace function
to determine the replacement."
  [^CharSequence s test-fn replace-fn]
  (let [l (.length s)]
    (loop [index (int 0)
           buffer (StringBuilder. l)]
      (if (= l index)
        (str buffer)
        (let [ch (.charAt s index)]
          (if (test-fn ch)
            (.append buffer (replace-fn ch))
            (.append buffer ch))
          (recur (inc index) buffer))))))


(defmacro string-or
  "Evaluates exprs one at a time, from left to right. If a form
returns a non-empty string, string-or returns that value and doesn't
evaluate any of the other expressions, otherwise it returns the
value of the last expression. (string-or) returns an empty string."
  {:added "1.0"}
  ([] "")
  ([x] x)
  ([x & next]
   `(let [or# ~x]
      (if (not (empty? or#)) or# (string-or ~@next)))))

(defn- try-body [body-fn]
  (try
    [::success (body-fn) nil]
    (catch java.lang.Exception e
      [::failed nil e])))


(defn retry* [body-fn body-str times delay delay-factor fail-fn ignore]
  (loop [count 1]
    (let [[t r f] (try-body body-fn)]
      (cond
        (= ::success t) r
        (= ::failed t) (if (<= count times)
                         (do
                           (when (and delay (pos? delay))
                             (let [dd (double (* delay (max 0 (if delay-factor (delay-factor count) 1))))
                                   d (Math/round dd)]
                               (try
                                 (log/info "Using a delay of" d " for next execution of:" body-str)
                                 (Thread/sleep d)
                                 (catch java.lang.Exception e))))
                           (recur (inc count)))
                         (do
                           (log/warn f "Failed" (dec count) "times to execute:" body-str)
                           (let [fr (when fail-fn (fail-fn f (dec count) body-str))]
                             (if ignore fr (throw f)))))))))

(defmacro retry
  "Evaluates the body of the macro. If an exception occurs it will retry the evaluation of the body after a certain delay.
(retry {:times 4 :delay 5000 :on-failure #(warn %1 \"Upload failed\" %2 \"times. Body: %3\") :ignore true}
       (upload/upload-mission-critical-stuff ...))"
  [{:keys [times delay delay-factor on-failure ignore] :or {delay 1000 delay-factor #(* 2 (Math/log %)) ignore false}} body]
  (if (pos? times)
    (let [retry-name (gensym "__retry_body_")
          body-str (str body)]
      `(letfn [(~retry-name [] ~body)]
         (retry* ~retry-name ~body-str ~times ~delay ~delay-factor ~on-failure ~ignore)))
    (throw (IllegalArgumentException. "The times parameter must be positive and greater than zero."))))

#_(defn different?
    "Checks whether two maps are substantially different. I.e., the version alone doesn't make it a different match."
    [map-a map-b & keys-to-ignore]
    (let [[things-only-in-a things-only-in-b things-in-both] (diff map-a map-b)]
      (or (not (empty? (apply dissoc things-only-in-a keys-to-ignore)))
          (not (empty? (apply dissoc things-only-in-b keys-to-ignore))))
      ))
