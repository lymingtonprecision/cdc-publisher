(ns cdc-publisher.system
  (:require [com.stuartsierra.component :as component]
            [environ.core :refer [env]]

            [cdc-util.components.database :refer [new-database-from-env]]
            [cdc-util.kafka :refer [default-control-topic]]

            [cdc-publisher.components.kafka-ccd-store :refer [kafka-ccd-store]]
            [cdc-publisher.components.kafka-queue-writer :refer [kafka-queue-writer]]
            [cdc-publisher.components.jms-publisher :refer [jms-publisher]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-system
  ([] (new-system env))
  ([env]
   (component/system-map
    :db
    (new-database-from-env env)
    :ccd-store
    (kafka-ccd-store
     (get env :control-topic default-control-topic)
     (:kafka-brokers env))
    :dst
    (kafka-queue-writer (:kafka-brokers env))
    :publisher
    (jms-publisher
     (select-keys env [:db-name :db-server :db-user :db-password])))))
