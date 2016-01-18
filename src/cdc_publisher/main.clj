(ns cdc-publisher.main
  (:require [clojure.tools.logging :as log]
            [clj-logging-config.log4j :refer [set-loggers!]]
            [com.stuartsierra.component :as component]
            [environ.core :refer [env]]
            [metrics.core :as metrics]
            [metrics.reporters :as metrics.reporter]
            [metrics.reporters.kafka :as metrics.kafka]
            [cdc-publisher.system :refer [new-system]])
  (:import org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.common.serialization.StringSerializer)
  (:gen-class))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Logging

(def timestamped-log-pattern "%d %-5p [%c] %m%n")

(defn init-logging! []
  (set-loggers!
   :root
   {:pattern timestamped-log-pattern
    :level :info}

   ["kafka"
    "org.apache.kafka"
    "org.apache.zookeeper"
    "org.I0Itec.zkclient"]
   {:name "Kafka"
    :level :error}

   "org.apache.zookeeper.ZooKeeper"
   {:name "ZooKeeper"
    :level :error}

   "com.zaxxer.hikari"
   {:name "HikariCP"
    :level :error})

  (Thread/setDefaultUncaughtExceptionHandler
   (reify Thread$UncaughtExceptionHandler
     (uncaughtException [_ thread ex]
       (log/error ex "Uncaught exception on" (.getName thread))))))

(defn init-metrics! []
  (metrics.reporter/start
   (metrics.kafka/reporter
    metrics/default-registry
    (KafkaProducer.
     {"bootstrap.servers" (:kafka-brokers env)}
     (StringSerializer.)
     (StringSerializer.)))
   10))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Main

(defn -main [& args]
  (init-logging!)
  (init-metrics!)
  (let [sys (component/start (new-system))]
    (.addShutdownHook (Runtime/getRuntime) (Thread. #(component/stop sys)))))
