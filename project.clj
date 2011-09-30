(defproject die-roboter "1.0.0-SNAPSHOT"
  :description "The robots get your work done in an straightforward way."
  :dependencies [[com.mefesto/wabbitmq "0.1.4"
                  :exclusions [org.clojure/clojure org.clojure/clojure-contrib]]
                 [org.clojure/tools.logging "0.2.0"
                  :exclusions [org.clojure/clojure]]]
  :checksum-deps true
  :local-repo-classpath true
  :dev-dependencies [[org.clojure/clojure "1.2.1"]])
