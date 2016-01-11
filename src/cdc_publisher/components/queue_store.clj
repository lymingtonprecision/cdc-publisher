(ns cdc-publisher.components.queue-store
  (:require [com.stuartsierra.component :as component]
            [clojure.java.jdbc :as jdbc :refer [with-db-connection
                                                with-db-transaction]]
            [yesql.util :refer [slurp-from-classpath]]
            [cdc-publisher.protocols.queue-store :refer [QueueStore]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Error codes

(def invalid-queue 24010)
(def dequeue-timeout 25228)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Query defs

(def queue-exists-sql (slurp-from-classpath "cdc_publisher/sql/queue_exists.sql"))
(def dequeue-msg-sql (slurp-from-classpath "cdc_publisher/sql/dequeue_message.sql"))

(defn -queue-exists?
  [db queue]
  (with-db-connection [db db]
    (with-open [stmt (doto (.prepareCall (:connection db) queue-exists-sql)
                       (.registerOutParameter 1 java.sql.Types/INTEGER)
                       (.setString 2 queue))]
      (.execute stmt)
      (pos? (.getInt stmt 1)))))

(defn -dequeue-msg
  ([db queue]
   (-dequeue-msg db queue identity))
  ([db queue cb]
   (with-db-transaction [db db]
     (with-open [stmt (doto (.prepareCall (:connection db) dequeue-msg-sql)
                        (.registerOutParameter 2 java.sql.Types/VARCHAR)
                        (.setString 1 queue))]
       (try
         (.execute stmt)
         (cb (.getString stmt 2))
         (catch java.sql.SQLException e
           (when-not (contains? #{invalid-queue dequeue-timeout} (.getErrorCode e))
             (throw e))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord IFSQueueStore [database]
  component/Lifecycle
  (start [this] this)
  (stop [this] this)

  QueueStore
  (queue-exists? [this queue]
    (-queue-exists? database queue))
  (dequeue! [this queue]
    (-dequeue-msg database queue))
  (dequeue-sync [this queue f]
    (-dequeue-msg database queue f)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-queue-store []
  (component/using
   (map->IFSQueueStore {})
   [:database]))
