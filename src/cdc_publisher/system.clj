(ns cdc-publisher.system
  (:require [com.stuartsierra.component :as component]
            [environ.core :refer [env]]

            [cdc-util.components.database :refer [new-database-from-env]]
            [cdc-util.kafka :refer [default-control-topic]]

            [cdc-publisher.components.ifs-queue-reader :refer [ifs-queue-reader]]
            [cdc-publisher.components.kafka-ccd-store :refer [kafka-ccd-store]]
            [cdc-publisher.components.kafka-queue-writer :refer [kafka-queue-writer]]
            [cdc-publisher.components.publisher :refer [publisher]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-system
  ([] (new-system env))
  ([env]
   (component/system-map
    :db (new-database-from-env env)
    :ccd-store (kafka-ccd-store
                (or (:control-topic env) default-control-topic)
                (:kafka-brokers env))
    :dst (kafka-queue-writer (:kafka-brokers env))
    :src (component/using
          (ifs-queue-reader nil)
          {:db-spec :db})
    :publisher (publisher))))
