(ns die.test.roboter
  (:refer-clojure :exclude [send-off])
  (:use [die.roboter]
        [clojure.test])
  (:require [com.mefesto.wabbitmq :as wabbit]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io])
  (:import (java.util.concurrent TimeUnit TimeoutException ExecutionException)
           (java.io IOException)
           (org.apache.log4j Level LogManager)))

(when-let [level (System/getenv "LOG_LEVEL")]
  (.setLevel (LogManager/getLogger "die.roboter")
             (Level/toLevel (.toUpperCase level))))

(def ^{:dynamic true} *timeout-expected* false)

(def state (atom {}))

(def bound :root)

(defn ack-handler [e msg]
  (com.mefesto.wabbitmq/ack (-> msg :envelope :delivery-tag)))

(defn clear-queues! [& queues]
  (with-robots {}
    (doseq [queue queues]
      (wabbit/with-queue queue
        (doall (take 100 (wabbit/consuming-seq true 1)))))))

(defn work-fixture [f]
  (clear-queues! "die.roboter.work" "die.roboter.response")
  (reset! state {})
  (f))

(defmacro with-worker [& body]
  `(let [worker# (future (work))]
     (try ~@body
          (finally (future-cancel worker#)))))

(defn wait-for [blockers]
  (try (.get (future (doseq [b blockers] @b)) 1 TimeUnit/SECONDS)
       (catch TimeoutException _
         (when (not *timeout-expected*)
           (is false "Timed out!")))))

(defmacro with-block [n body]
  `(let [blockers# (repeatedly ~n #(promise))
         blocked# (atom blockers#)]
     (add-watch state :unblocker (fn [& _#]
                                   (locking blocked#
                                     (if (seq @blocked#)
                                       (deliver (first @blocked#) true)
                                       (println "Unblocked too many times!"))
                                     (swap! blocked# rest))))
     (try
       ~body
       (wait-for (take ~n blockers#))
       (finally (remove-watch state :unblocker)))))

(use-fixtures :each work-fixture)

(deftest test-send-off
  (with-worker
    (with-block 1
      (send-off `(swap! state assoc :ran true))))
  (is (:ran @state)))

(deftest test-send-back
  (with-worker
    (Thread/sleep 50) ; allow the workers to spin up
    (is (= 1 (deref (send-back 1) 100 :timeout)))
    (is (= 2 (deref (send-back `(+ 1 1)) 100 :timeout)))))

;; TODO: still too much nondeterminism here.
;; (deftest ^:broadcast test-simple-broadcast
;;   (let [worker (future
;;                  (binding [bound 1]
;;                    (work-on-broadcast)))]
;;     (Thread/sleep 100)
;;     (try
;;       (with-block 1
;;         (broadcast `(swap! state assoc bound true)))
;;       (finally (future-cancel worker))))
;;   (is (= {1 true} @state)))

;; (deftest ^:broadcast test-multiple-broadcast
;;   (let [worker1 (future
;;                   (binding [bound 1]
;;                     (work-on-broadcast {:queue "worker1"})))
;;         worker2 (future
;;                   (binding [bound 2]
;;                     (work-on-broadcast {:queue "worker2"})))]
;;     (Thread/sleep 100)
;;     (try
;;       (with-block 2
;;         (broadcast `(swap! state assoc bound true)))
;;       (finally (future-cancel worker1)
;;                (future-cancel worker2))))
;;   (is (= {1 true 2 true} @state)))

(deftest test-timeout-normal-copy
  (let [worker (future
                 (binding [*exception-handler* ack-handler]
                   (work {:timeout 100})))]
    (try
      (binding [*timeout-expected* true]
        (with-block 1
          (send-off `(do (io/copy (io/file "/etc/hosts") (io/file "/dev/null"))
                         (Thread/sleep 2000)
                         (swap! state assoc :ran true)))))
      (finally (future-cancel worker)))
    (is (not (:ran @state)))))

(deftest test-progressive-copy
  (let [worker (future
                 (binding [*exception-handler* ack-handler]
                   (work {:timeout 100})))]
    (try
      (with-block 1
        (send-off `(do (copy (io/file "/etc/hosts") (io/file "/tmp/hosts"))
                       (swap! state assoc :ran true))))
      (finally (future-cancel worker)))
    (is (:ran @state))))

(deftest ^:exceptions test-exception-over-future
  (let [worker (future
                 (binding [*exception-handler* ack-handler]
                   (work {:timeout 100})))]
    (try
      (is (instance? IOException
                     (deref (send-back '(throw (java.io.IOException.)))
                            100 :timeout)))
      (finally (future-cancel worker)))))
