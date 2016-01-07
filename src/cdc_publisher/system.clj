(ns cdc-publisher.system
  (:require [com.stuartsierra.component :as component]
            [environ.core :refer [env]]
            [cdc-util.components.database :refer [new-database-from-env]]
            [cdc-util.components.kafka :refer [new-kafka]]
            [cdc-util.kafka :refer [default-control-topic]]
            [cdc-publisher.components.publisher :refer [new-publisher]]
            [cdc-publisher.components.queue-store :refer [new-queue-store]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-system
  ([] (new-system env))
  ([env]
   (component/system-map
    :database (new-database-from-env env)
    :kafka (new-kafka (:zookeeper env) "cdc-publisher")
    :queue-store (new-queue-store)
    :publisher (new-publisher (or (:control-topic env) default-control-topic)))))
