(ns die.test.roboter
  (:refer-clojure :exclude [future send-off])
  (:use [die.roboter]
        [clojure.test])
  (:require [com.mefesto.wabbitmq :as wabbit]
            [clojure.tools.logging :as log])
  (:import (java.util.concurrent TimeUnit)))

(.setLevel (java.util.logging.Logger/getLogger "die.roboter")
           java.util.logging.Level/ALL) ; TODO: no-op

(def state (atom {}))

(def bound :root)

(defn clear-queues! [queue-name]
  (with-robots {}
    (wabbit/with-queue queue-name
      (doall (take 100 (wabbit/consuming-seq true 1))))))

(defn work-fixture [f]
  (clear-queues! "die.roboter.work")
  (reset! state {})
  (f))

(defmacro with-worker [& body]
  `(let [worker# (clojure.core/future (work))]
     (try ~@body
          (finally (.cancel worker# true)))))

(defn wait-for [blockers]
  (try (.get (clojure.core/future (doseq [b blockers] @b)) 1 TimeUnit/SECONDS)
       (catch java.util.concurrent.TimeoutException _
         (is false "Timed out!"))))

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

(deftest test-future
  (with-worker
    (is (= 1 (.get (future 1) 100 TimeUnit/MILLISECONDS)))))

(deftest test-simple-broadcast
  (with-block 1
    (let [worker (clojure.core/future
                  (binding [bound 1]
                    (work-on-broadcast)))]
      (Thread/sleep 100)
      (try (broadcast `(swap! state assoc bound true))
           (finally (.cancel worker true)))))
  (is (= {1 true} @state)))

(deftest test-multiple-broadcast
  (with-block 2
    (let [worker1 (clojure.core/future
                   (binding [bound 1]
                     (work-on-broadcast {:queue "worker1"})))
          worker2 (clojure.core/future
                   (binding [bound 2]
                     (work-on-broadcast {:queue "worker2"})))]
      (Thread/sleep 100)
      (try (broadcast `(swap! state assoc bound true))
           (finally (.cancel worker1 true)
                    (.cancel worker2 true)))))
  (is (= {1 true 2 true} @state)))
