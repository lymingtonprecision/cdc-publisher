(ns cdc-publisher.components.ifs-queue-reader
  (:require [clojure.java.jdbc :as jdbc :refer [with-db-transaction]]
            [com.stuartsierra.component :as component]
            [metrics.meters :as meter :refer [meter]]
            [metrics.timers :as timer :refer [timer time!]]
            [yesql.util :refer [slurp-from-classpath]]
            [cdc-publisher.core :refer [*metrics-group* dml->msg]]
            [cdc-publisher.protocols.queue :refer [QueueReader]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Metrics

(defn dequeue-count
  ([]
   (meter [*metrics-group* "total" "dequeue-count"]))
  ([queue]
   (meter [*metrics-group* (str "queue." queue) "dequeue-count"])))

(defn dequeue-timer
  ([]
   (timer [*metrics-group* "total" "dequeue-time"]))
  ([queue]
   (timer [*metrics-group* (str "queue." queue) "dequeue-time"])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants

(def dequeue-timeout 25228)
(def invalid-queue 24010)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(def dequeue-msg-sql (slurp-from-classpath "cdc_publisher/sql/dequeue_message.sql"))

(defn- dequeue-msg!
  [db-conn queue]
  (with-open [stmt (doto (.prepareCall (:connection db-conn) dequeue-msg-sql)
                     (.setString 1 queue)
                     (.registerOutParameter 2 java.sql.Types/VARCHAR))]
    (.execute stmt)
    (.getString stmt 2)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord IFSQueueReader [db-spec]
  QueueReader
  (dequeue! [this queue]
    (.dequeue-sync this queue identity))
  (dequeue-sync [this queue f]
    (let [ts (doall (map timer/start [(dequeue-timer queue) (dequeue-timer)]))]
      (try
        (with-db-transaction [db db-spec]
          (when-let [dml (dequeue-msg! db queue)]
            (doseq [m [(dequeue-count queue) (dequeue-count)]] (meter/mark! m))
            (f (dml->msg dml))))
        (catch java.sql.SQLException e
          (when (contains? #{dequeue-timeout invalid-queue} (.getErrorCode e))
            nil))
        (finally
          (doseq [t ts] (timer/stop t)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(def ifs-queue-reader ->IFSQueueReader)
