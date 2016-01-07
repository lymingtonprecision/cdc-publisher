(ns cdc-publisher.main
  (:require [clojure.tools.logging :as log]
            [clj-logging-config.log4j :refer [set-loggers!]]
            [com.stuartsierra.component :as component]
            [cdc-publisher.system :refer [new-system]])
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Main

(defn -main [& args]
  (init-logging!)
  (let [sys (component/start (new-system))]
    (.addShutdownHook (Runtime/getRuntime) (Thread. #(component/stop sys)))))
