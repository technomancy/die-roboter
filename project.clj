(defproject die-roboter "1.0.0-SNAPSHOT"
  :description "The robots get your work done in an straightforward way."
  :dependencies [[org.clojure/clojure "[1.2.0,1.3.0]"]
                 [commons-codec "1.5"]
                 [com.mefesto/wabbitmq "0.1.4"
                  :exclusions [org.clojure/clojure-contrib]]
                 [org.clojure/tools.logging "0.2.0"]]
  :checksum-deps true
  :local-repo-classpath true)
