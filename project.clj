(defproject gorillalabs/eventsourcing "0.1.0-SNAPSHOT"
  :description "A Clojure event-sourcing abstraction on configurable storage backends."
  :url "https://github.com/gorillalabs/eventsourcing"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clj-time "0.11.0"]

                 ;; Logging
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-api "1.7.19"]
                 [org.slf4j/log4j-over-slf4j "1.7.19"]
                 [ch.qos.logback/logback-core "1.1.5"]
                 [ch.qos.logback/logback-classic "1.1.5"]

                 ;; mongo
                 [congomongo "0.4.8"]

                 ;; testing
                 [juxt/iota "0.2.2"]
                 ])
