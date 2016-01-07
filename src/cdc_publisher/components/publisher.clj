(ns cdc-publisher.components.publisher
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]

            [clj-kafka.new.producer :as kafka]
            [clj-time.core :as time]

            [cdc-util.async :refer [go-till-closed]]
            [cdc-util.filter :as filter]
            [cdc-util.kafka :refer :all]

            [cdc-publisher.core :refer [dml->msg]]
            [cdc-publisher.components.queue-store :as queue-store]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(defn ccds-to-publish
  "Reads all messages posted to the specified control topic and
  returns the subset that represent Change Capture Definitions
  that are ready to publish.

  Resets the consumer offset for the topic to the maximum offset
  read."
  [kafka-config topic]
  (filter
   #(= :active (:status %))
   (topic->last-known-ccd-states kafka-config topic)))

(defn some-references-missing?
  "Returns a collection of keys indicating those references from the
  given Change Capture Definition that are missing from the provided
  stores, or `nil` if all references are valid."
  [{:keys [queue] :as ccd} queue-store kafka]
  (-> []
      (#(if (queue-store/queue-exists? queue-store queue) % (conj % :queue)))
      (#(if (topic-exists? (:config kafka) queue) % (conj % :topic)))
      seq))

(defn filter-ccds-for-publication
  "Returns a transducer that filters out any received Change Capture
  Definitions that are not valid for publication.

  Calls `fail-fn` with any definitions that fail validation, updated
  with an appropriate error status."
  [queue-store kafka fail-fn]
  (fn [xf]
    (fn
      ([] (xf))
      ([rs] (xf rs))
      ([rs ccd]
       (if-let [missing (some-references-missing? ccd queue-store kafka)]
         (do
           (fail-fn
            (merge
             ccd
             {:status :error
              :timestamp (time/now)
              :error {:type "PublicationError"
                      :message (str "missing " missing)}}))
           rs)
         (xf rs ccd))))))

(defn create-publication!
  [{:keys [queue] :as ccd} queue-store kafka]
  (queue-store/dequeue-loop
   queue-store
   queue
   (fn [dml]
     (let [{:keys [key value]} (dml->msg dml)]
       @(kafka/send (:producer kafka) (kafka/record queue key value))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord Publisher [control-topic kafka queue-store]
  component/Lifecycle
  (start [this]
    (if (:publications this)
      this
      (do
        (log/info "starting publisher")
        (when-not (topic-exists? (:config kafka) control-topic)
          (create-control-topic! (:config kafka) control-topic))
        (let [ccds (ccds-to-publish (:config kafka) control-topic)
              _ (log/info "found" (count ccds) "CCDs ready to publish")
              ;; seed the publication queue with all currently active CCDs
              publication-queue (async/chan
                                 100
                                 (filter-ccds-for-publication
                                  queue-store
                                  kafka
                                  (fn [ccd]
                                    (send-ccd (:producer kafka) control-topic ccd))))
              _ (async/onto-chan publication-queue ccds false)
              ;; put any new CCD activations onto the publication queue
              activations (async/chan
                           1
                           (filter/msgs->ccds-with-status :active))
              _ (async/pipe activations publication-queue)
              ccd-loop (ccd-topic-onto-chan activations control-topic (:config kafka))
              ;; create and track our publications
              publications (atom {})
              publication-loop
              (go-till-closed
               publication-queue
               (fn [{:keys [queue] :as ccd} _]
                 (if (get @publications queue)
                   (log/warn "already publishing" queue)
                   (do
                     (swap! publications
                            assoc queue
                            (create-publication! ccd queue-store kafka))
                     (log/info "started publishing" queue)))))]
          (assoc this
                 :queue publication-queue
                 :publications publications
                 :ccd-loop ccd-loop
                 :publication-loop publication-loop)))))
  (stop [this]
    (if (:publications this)
      (do
        (doseq [[k l] (select-keys this [:ccd-loop :publication-loop])]
          (log/info "terminating loop" k)
          (.close l))
        (doseq [[k l] @(:publications this)]
          (log/info "stopping publication of" k)
          (.close l))
        (log/info "stopped publisher")
        (dissoc this :queue :publications :ccd-loop :publication-loop))
      this)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-publisher [control-topic]
  (component/using
   (map->Publisher {:control-topic control-topic})
   [:kafka :queue-store]))
