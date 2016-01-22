(ns cdc-publisher.components.jms-publisher
  (:require [clojure.core.async :as async]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [cdc-publisher.core :refer [dml->msg]]
            [cdc-publisher.protocols.queue :as queue]
            [cdc-publisher.protocols.ccd-store :as ccd-store :refer [CCDStore]])
  (:import javax.jms.Session
           javax.jms.MessageListener
           oracle.jms.AQjmsFactory
           oracle.sql.ORADataFactory))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility records

(defrecord QueueMessage [format data queued-at]
  oracle.sql.ORAData
  (toDatum [this c]
    nil))

(defrecord JmsQueuePublisher [queue session receiver]
  java.io.Closeable
  (close [this]
    (log/debug (str "stopping publication of " queue))
    (.close receiver)
    (.close session)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(def ora-data->queue-message
  (reify oracle.sql.ORADataFactory
    (create [this datum type]
      (apply ->QueueMessage (.getAttributes datum)))))

(defn >!active-ccds
  "Publishes all known active Change Capture Definitions from the
  controller to `ch` and continuously posts newly activated CCDs until
  `ch` is closed."
  [store ch]
  {:pre [(satisfies? CCDStore store)]}
  (let [[ccds offset] (ccd-store/known-ccds store)
        active-ccd-ch (async/chan 1 (filter #(= :active (:status %))))]
    ;; TODO validate CCDs and check for existence of queue/topic, reject invalid ones
    (async/pipe active-ccd-ch ch false)
    (async/onto-chan active-ccd-ch ccds false)
    (ccd-store/post-updates-to-chan store active-ccd-ch (inc offset))))

(defn read-clob
  [clob]
  (let [sb (StringBuilder.)]
    (with-open [r (.getCharacterStream clob)]
      (loop [c (.read r)]
        (if (neg? c)
          (str sb)
          (do
            (.append sb (char c))
            (recur (.read r))))))))

(defn queue-publisher
  [conn queue-name dst]
  (log/debug (str "creating publisher for " queue-name))
  (let [s (.createQueueSession conn false Session/AUTO_ACKNOWLEDGE)
        [schema queue] (string/split queue-name #"\.")
        q (.getQueue s schema queue)
        r (doto (.createReceiver s q nil ora-data->queue-message)
            (.setMessageListener
             (reify MessageListener
               (onMessage [this message]
                 (try
                   (let [parsed-message
                         (-> message
                             .getAdtPayload
                             :data
                             read-clob
                             dml->msg)]
                     (queue/enqueue! dst queue-name parsed-message))
                   (catch Exception e
                     (log/error (str "error processing message from " queue-name) e)
                     (throw e)))))))]
    (->JmsQueuePublisher queue-name s r)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord JmsPublisher [db-spec ccd-store dst]
  component/Lifecycle
  (start [this]
    (if (some-> this :active? deref)
      this
      (let [active? (atom true)
            qcf (AQjmsFactory/getQueueConnectionFactory
                 (:db-server db-spec)
                 (:db-name db-spec)
                 (get db-spec :db-port 1521)
                 "thin")
            qc (doto (.createQueueConnection
                      qcf
                      (:db-user db-spec)
                      (:db-password db-spec))
                 .start)
            queues (atom {})
            queue-chan (async/chan 10 (map :queue))
            queue-enabler (async/go-loop []
                            (when-let [q (async/<! queue-chan)]
                              (swap! queues assoc q (queue-publisher qc q dst))
                              (recur)))]
        (log/info "starting JMS publisher")
        (>!active-ccds ccd-store queue-chan)
        (assoc this
               :active? active?
               :connection qc
               :chan queue-chan
               :queues queues))))
  (stop [this]
    (log/info "stopping JMS publisher")
    (when-let [a (:active? this)]
      (reset! a false))
    (when-let [ch (:chan this)]
      (async/close! ch))
    (when-let [qs (some-> this :queues deref vals)]
      (doseq [q qs]
        (.close q)))
    (when-let [qc (:connection this)]
      (.close qc))
    (dissoc this :active? :chan :queues)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn jms-publisher [db-spec]
  (component/using
   (map->JmsPublisher {:db-spec db-spec})
   [:ccd-store :dst]))
