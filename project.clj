(defproject gorillalabs/eventsourcing "0.1.1-SNAPSHOT"
  :description "A Clojure event-sourcing abstraction on configurable storage backends."
  :url "https://github.com/gorillalabs/eventsourcing"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clj-time "0.11.0"]

                 ;; Logging
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-api "1.7.21"]
                 [org.slf4j/log4j-over-slf4j "1.7.21"]
                 [ch.qos.logback/logback-core "1.1.7"]
                 [ch.qos.logback/logback-classic "1.1.7"]

                 ;; MongoDB support ;; this is commented at the moment because we do not need Mongo support at the moment and I don't want to support it.
                 [congomongo "0.4.8"]

                 ;; Cassandra support ;; this is commented at the moment because we do not have Cassandra persistence at the moment.
                 [clojurewerkz/cassaforte "2.0.0"]

                 ;; testing
                 [juxt/iota "0.2.3"]
                 ]
  :exclusions [org.clojure/clojure
               org.slf4j/slf4j-nop
               org.slf4j/slf4j-log4j12
               log4j
               commons-logging/commons-logging]

  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]]
                   :plugins      [[jonase/eastwood "0.2.3" :exclusions [org.clojure/clojure]]
                                  [lein-ancient "0.6.10" :exclusions [org.clojure/clojure]]
                                  [lein-pprint "1.1.1" :exclusions [org.clojure/clojure]]
                                  [lein-ns-dep-graph "0.1.0-SNAPSHOT"]]}}
  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "v"]
                  ["deploy"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]])
