(ns die.roboter
  "The Robots get your work done in an straightforward way."
  (:refer-clojure :exclude [future send-off])
  (:require [com.mefesto.wabbitmq :as wabbit]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.walk :as walk])
  (:import (java.util UUID)
           (java.util.concurrent Executors TimeUnit TimeoutException)
           (java.lang.management ManagementFactory)
           (java.io FilterInputStream)))

(def ^{:doc "Namespace in which robots work." :private true} context
  (binding [*ns* (create-ns 'die.roboter.context)] (refer-clojure) *ns*))

(def ^{:doc "Message being evaled by worker." :dynamic true} *current-message*)

(defn ^{:dynamic true} *exception-handler*
  "Default exception handler simply logs. Rebind to perform your own recovery."
  [e msg]
  (log/warn e "Robot ran into trouble:" (String. (:body msg))))

(def ^{:doc "How long before jobs that don't report progress are killed, in ms."
       :dynamic true} *timeout*
  (* 1000 60 5)) ; five minutes

(defn ^{:dynamic true :doc "Reset job timeout."} report-progress [])

(defn ^{:internal true :doc "Public for macro-expansion only!"} init [config]
  (try (wabbit/exchange-declare (:exchange config "die.roboter")
                                (:exchange-type config "direct")
                                (:exchange-durable config true)
                                (:exchange-auto-delete config false))
       (wabbit/queue-declare (:queue config "die.roboter.work")
                             (:durable config true)
                             (:exclusive config false))
       (wabbit/queue-bind (:queue config "die.roboter.work")
                          (:exchange config "die.roboter")
                          (:queue config "die.roboter.work"))
       (catch Exception e
         (log/error e "Couldn't declare exchange/queue."))))

(alter-var-root #'init memoize)

(def ^{:dynamic true} *config* nil)

(defmacro with-robots [config & body]
  ;; :implicit should only start a new connection if there's none active.
  `(if (or (and *config* (:implicit ~config))
           (= *config* ~config)) ; avoid redundant nesting
     (do ~@body)
     (binding [*config* ~config]
       (wabbit/with-broker ~config
         (wabbit/with-channel ~config
           (init ~config)
           (wabbit/with-exchange (:exchange ~config)
             ~@body))))))

(defn send-off
  "Execute a form on a robot node."
  ([form] (send-off form {}))
  ([form config]
     (with-robots (merge {:implicit true} config)
       (log/trace "Published" (pr-str form) (:key config "die.roboter.work"))
       (wabbit/publish (:key config "die.roboter.work")
                       (.getBytes (pr-str form))))))

(defn broadcast
  "Like send-off, but the form runs on all robot nodes."
  ([form] (broadcast form {}))
  ([form config]
     (send-off form (merge {:exchange "die.roboter.broadcast"
                            :exchange-type "fanout"
                            :key "die.roboter.broadcast"} config))))

(defmacro future
  "Run body on a robot node and return a result upon deref."
  [& body]
  `(let [reply-queue# (format "die.roboter.reply.%s" (UUID/randomUUID))]
     (clojure.core/future
      (with-robots (merge {:implict true} *config*)
        (wabbit/queue-declare reply-queue# false true true)
        (send-off (list `wabbit/publish reply-queue#
                        '(.getBytes (pr-str (do ~@body)))))
        (wabbit/with-queue reply-queue#
          (-> (wabbit/consuming-seq true) first :body String. read-string))))))

(defn- success? [f timeout]
  (try (.get f timeout TimeUnit/MILLISECONDS) true
       ;; TODO: get stack trace if there's an exception inside the future
       (catch TimeoutException _)))

(defn- supervise [f progress timeout]
  (when-not (success? f timeout)
    (if @progress
      (do (reset! progress false)
          (recur f progress timeout))
      (future-cancel f))))

(defn- run-with-timeout [timeout f & args]
  (let [progress (atom false)
        f-fn (bound-fn [] (apply f args))
        fut (clojure.core/future ; TODO: name thread
             (binding [report-progress (fn [] (reset! progress true))]
               (f-fn)))]
    (-> #(supervise fut progress timeout) (Thread.) .start)
    fut))

(defn- consume [{:keys [body envelope] :as msg} timeout]
  (binding [*ns* context,*current-message* msg]
    (log/trace "Robot received message:" (String. body))
    (run-with-timeout timeout eval (read-string (String. body))))
  (wabbit/ack (:delivery-tag envelope)))

(defn work
  "Wait for work and eval it continually."
  ([config]
     (with-robots config
       (wabbit/with-queue (:queue config "die.roboter.work")
         (log/trace "Consuming on" (:queue config "die.roboter.work"))
         (doseq [msg (wabbit/consuming-seq)]
           (try (consume msg (:timeout config *timeout*))
                (catch Exception e
                  (*exception-handler* e msg)))))))
  ([] (work {:implicit true})))

(defn work-on-broadcast
  "Wait for work on the broadcast queue and eval it continually."
  ([config]
     (work (merge {:exchange "die.roboter.broadcast", :exchange-type "fanout"
                   :exclusive true
                   :queue (str "die.roboter.broadcast." (UUID/randomUUID))}
                  config)))
  ([] (work-on-broadcast {:implicit true})))

(defn- progressive-input [input]
  ;; TODO: this fails without the erronous hint
  (let [ins (io/input-stream ^java.io.File input)]
    (proxy [FilterInputStream] [ins]
      (read [bytes]
        (report-progress)
        (.read ins bytes))
      (close []
        (.close ins)
        (proxy-super close)))))

(defn copy
  "Copy between input and output using clojure.java.io/copy, but reporting
   progress every so often. Use to prevent long IO operations from timing out."
  [input output & opts]
  (apply io/copy (progressive-input input) output opts))

(def workers (atom ()))

(defn add-worker
  "Spin up a worker with the given options."
  [opts]
  (swap! workers conj (clojure.core/future (work opts))))

(defn stop-worker
  "Cancel the most recently-created worker."
  []
  (swap! workers (fn [[worker & others]]
                   (future-cancel worker)
                   others)))

(defn -main [& {:as opts}]
  (let [opts (into {:workers (Integer. (or (System/getenv "WORKER_COUNT") 4))}
                   (walk/keywordize-keys opts))]
    (dotimes [n (opts :workers)] (add-worker opts))))