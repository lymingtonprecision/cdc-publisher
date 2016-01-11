(ns cdc-publisher.components.kafka-ccd-store
  (:require [clojure.core.async :as async]
            [com.stuartsierra.component :as component]
            [cdc-util.format :as format]
            [cdc-publisher.protocols.ccd-store :refer [CCDStore]])
  (:import org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.consumer.ConsumerRecord
           org.apache.kafka.clients.consumer.ConsumerRecords
           org.apache.kafka.common.TopicPartition
           org.apache.kafka.common.serialization.Deserializer
           org.apache.kafka.common.serialization.StringDeserializer))

(def ^:dynamic *default-poll-timeout* 1000)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(defn ccd-deserializer
  "Returns a Kafka `Deserializer` instance that deserializes the values
  of Kafka `ConsumerRecord`s into Change Capture Definition maps."
  []
  (reify Deserializer
    (configure [this opts key?])
    (close [this])
    (deserialize [this topic data]
      (some-> data (String. "UTF-8") format/json-str->ccd))))

(defn last-known-values
  "Returns a collection of the highest offset value for each key in
  the given set of `ConsumerRecords`."
  [^ConsumerRecords crs]
  (doall
   (map
    (fn [[_ [v _]]] v)
    (reduce
     (fn [rs ^ConsumerRecord cr]
       (let [[v o] (get rs (.key cr) [nil -1])]
         (if (> (.offset cr) o)
           (assoc rs (.key cr) [(.value cr) (.offset cr)])
           rs)))
     {}
     crs))))

(defn consumer
  "Returns a new `KafkaConsumer` connection to the specified servers,
  deserializing message keys as strings and values as Change Data
  Capture definition maps."
  ([bootstrap-servers]
   (consumer bootstrap-servers :latest))
  ([bootstrap-servers default-position]
   (KafkaConsumer.
    {"bootstrap.servers" bootstrap-servers
     "enable.auto.commit" "false"
     "auto.offset.reset" (name default-position)}
    (StringDeserializer.)
    (ccd-deserializer))))

(defn assign-topic!
  "Assigns all partitions of the specified topic to the given Kafka
  consumer so that it may be polled.

  If an `offset` is provided then the consumer will be `seek`ed to
  that offset, otherwise the default Kafka positioning for the
  consumer will be used."
  ([consumer topic]
   (assign-topic! consumer topic nil))
  ([consumer topic offset]
   (let [partitions (map #(TopicPartition. (.topic %) (.partition %))
                         (.partitionsFor consumer topic))]
     (.assign consumer partitions)
     (when offset
       (doseq [tp partitions]
         (.seek consumer tp offset))))))

(defn message-loop
  "Returns a `go` block which continuously polls the specified topic
  for new messages and calls `(f value offset)` on them.

  The loop will terminate when either:

  * `(continue?)` returns false.
  * `(f value offset)` returns a non-truthy value for any message."
  [topic bootstrap-servers starting-offset f continue?]
  (async/go
    (with-open [c (consumer bootstrap-servers :latest)]
      (assign-topic! c topic starting-offset)
      (loop []
        (when (reduce
               (fn [success? cr]
                 (when (and (continue?) success?)
                   (f (.value cr) (.offset cr))))
               true
               (.poll c *default-poll-timeout*))
          (when (continue?) (recur)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Components

(defrecord UpdatePoster [ch topic bootstrap-servers starting-offset]
  component/Lifecycle
  (start [this]
    (if (and (:active? this) @(:active? this))
      this
      (let [active? (atom true)
            offset (or (:offset this) (atom starting-offset))
            thread (message-loop
                    topic
                    bootstrap-servers
                    @offset
                    (fn [ccd ccd-offset]
                      (when (async/>!! ch ccd)
                        (reset! offset ccd-offset)))
                    #(deref active?))]
        (assoc this
               :active? active?
               :offset offset
               :thread thread))))
  (stop [this]
    (when-let [a (:active? this)]
      (reset! a false))
    (assoc this :active? nil :thread nil))

  java.io.Closeable
  (close [this]
    (.stop this)))

(defrecord KafkaCCDStore [topic bootstrap-servers]
  CCDStore
  (known-ccds [this]
    (with-open [c (consumer bootstrap-servers :earliest)]
      (assign-topic! c topic)
      (let [rs (.poll c *default-poll-timeout*)]
        [(last-known-values rs) (apply max (map #(.offset %) rs))])))
  (post-updates-to-chan [this ch]
    (.post-updates-to-chan this ch nil))
  (post-updates-to-chan [this ch as-of]
    (.start (->UpdatePoster ch topic bootstrap-servers as-of))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(def kafka-ccd-store ->KafkaCCDStore)
