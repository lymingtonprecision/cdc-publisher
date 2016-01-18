(ns cdc-publisher.components.kafka-queue-writer
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [metrics.meters :as meter :refer [meter]]
            [metrics.timers :as timer :refer [timer time!]]
            [cdc-publisher.core :refer [*metrics-group*]]
            [cdc-publisher.protocols.queue :as queue])
  (:import java.lang.Math
           java.util.concurrent.TimeUnit
           java.util.concurrent.ExecutionException
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.common.serialization.StringSerializer
           org.apache.kafka.common.errors.RetriableException))

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

(defn disconnect!
  [producer]
  (.close producer *default-close-timeout-ms* TimeUnit/MILLISECONDS))

(defn reconnect!
  [queue-writer]
  (swap!
   (:producer queue-writer)
   (fn [p]
     (future (disconnect! p))
     (producer (:bootstrap-servers queue-writer)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord KafkaQueueWriter [bootstrap-servers]
  component/Lifecycle
  (start [this]
    (if (:producer this)
      this
      (assoc this :producer (atom (producer bootstrap-servers)))))
  (stop [this]
    (when-let [p (some-> this :producer deref)]
      (disconnect! p))
    (assoc this :producer nil))

  queue/QueueWriter
  (reset! [this]
    (reconnect! this))
  (enqueue! [this queue {:keys [key value] :as msg}]
    (try
      (let [producer @(:producer this)
            t (timer/start (enqueue-timer))]
        @(.send producer (ProducerRecord. queue key value))
        (timer/stop t)
        (meter/mark! (enqueue-count)))
      (catch ExecutionException e
        (if (instance? RetriableException (.getCause e))
          (queue/*retriable-enqueue-error*
           (.getMessage e)
           {})
          (queue/*enqueue-error*
           (.getMessage e)
           {:queue queue
            :msg msg
            :ex (.getCause e)}))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(def kafka-queue-writer ->KafkaQueueWriter)
