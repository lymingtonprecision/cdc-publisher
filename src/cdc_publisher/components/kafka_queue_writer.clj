(ns cdc-publisher.components.kafka-queue-writer
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [metrics.meters :as meter :refer [meter]]
            [metrics.timers :as timer :refer [timer time!]]
            [cdc-publisher.core :refer [*metrics-group*]]
            [cdc-publisher.protocols.queue :refer [QueueWriter]])
  (:import java.util.concurrent.TimeUnit
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.common.serialization.StringSerializer))

(def ^:dynamic *default-close-timeout-ms* (* 10 1000))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Metrics

(defn enqueue-count
  ([]
   (meter [*metrics-group* "total" "enqueue-count"]))
  ([queue]
   (meter [*metrics-group* (str "queue." queue) "enqueue-count"])))

(defn enqueue-timer
  ([]
   (timer [*metrics-group* "total" "enqueue-time"]))
  ([queue]
   (timer [*metrics-group* (str "queue." queue) "enqueue-time"])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(comment
  (defn json-serializer
   []
   (reify Serializer
     (configure [this opts key?])
     (close [this])
     (serialize [this topic data]
       (some-> data cheshire/generate-string .getBytes)))))

(defn producer
  [bootstrap-servers]
  (KafkaProducer.
   {"bootstrap.servers" bootstrap-servers}
   (StringSerializer.)
   (StringSerializer.)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord KafkaQueueWriter [bootstrap-servers]
  component/Lifecycle
  (start [this]
    (if (:producer this)
      this
      (assoc this :producer (producer bootstrap-servers))))
  (stop [this]
    (when-let [p (:producer this)]
      (.close p *default-close-timeout-ms* TimeUnit/MILLISECONDS))
    (assoc this :producer nil))

  QueueWriter
  (enqueue! [this queue {:keys [key value] :as msg}]
    (let [producer (:producer this)
          ts (doall (map timer/start [(enqueue-timer queue) (enqueue-timer)]))]
      @(.send producer (ProducerRecord. queue key value))
      (doseq [t ts] (timer/stop t))
      (doseq [m [(enqueue-count queue) (enqueue-count)]] (meter/mark! m)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(def kafka-queue-writer ->KafkaQueueWriter)
