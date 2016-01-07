(ns user
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]

            [reloaded.repl :refer [system init start stop go reset]]
            [environ.core]

            [clj-logging-config.log4j :refer [set-loggers!]]

            [cdc-publisher.system :refer [new-system]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Logging

(def timestamped-log-pattern "%d %-5p - %m%n")

(set-loggers!
 :root
 {:pattern "%d{HH:mm:ss.SSS} %-5p [%c] %m%n"
  :level :debug}

 ["kafka"
  "org.apache.kafka"
  "org.apache.zookeeper"
  "org.I0Itec.zkclient"]
 {:name "Kafka"
  :out (clojure.java.io/as-file "logs/kafka.log")
  :pattern timestamped-log-pattern
  :append false}

 "org.apache.zookeeper.ZooKeeper"
 {:level :off}

 "com.zaxxer.hikari"
 {:name "HikariCP"
  :out (clojure.java.io/as-file "logs/hikaricp.log")
  :pattern timestamped-log-pattern
  :append false})

(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread ex]
     (log/error "Uncaught exception on" (.getName thread) ex))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ENV reloading

(in-ns 'environ.core)

(defn refresh-env
  "Hack to enable in-repl refresh of the environment vars"
  []
  (def env
    (merge (read-env-file)
           (read-system-env)
           (read-system-props))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; System reloading

(in-ns 'user)

(reloaded.repl/set-init!
 (fn []
   (environ.core/refresh-env)
   (new-system)))
