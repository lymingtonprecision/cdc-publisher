(ns cdc-publisher.components.queue-store
  (:require [clojure.core.async :as async]
            [clojure.java.jdbc :as jdbc :refer [with-db-connection]]
            [clojure.string :as string]
            [com.stuartsierra.component :as component]
            [yesql.core :refer [defquery]]
            [yesql.util :refer [slurp-from-classpath]]
            [cdc-publisher.retry :refer [retry-loop]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(defn basename
  "Returns the unqualified name of the queue (stripped of its
  schema.)"
  [q]
  (-> q (string/split #"\.") last string/lower-case))

(defn has-error-code?
  "Returns true if the given exception has the specified error code."
  [e c]
  (and e (= c (.getErrorCode e))))

(def dequeue-timeout 25228)
(def session-killed 28)
(def connection-lost 17008)

(defn dequeue-timeout? [e] (has-error-code? e dequeue-timeout))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Query defs

(def queue-exists-sql (slurp-from-classpath "cdc_publisher/sql/queue_exists.sql"))
(def dequeue-msg-sql (slurp-from-classpath "cdc_publisher/sql/dequeue_message.sql"))

(defn -queue-exists?
  [db queue]
  (with-db-connection [db db]
    (let [stmt (doto (.prepareCall (:connection db) queue-exists-sql)
                 (.registerOutParameter 1 java.sql.Types/INTEGER))]
      (.setString stmt 2 queue)
      (.execute stmt)
      (pos? (.getInt stmt 1)))))

(defn dequeue-msg-stmt
  "Returns a prepared JDBC statement to dequeue messages from the
  specified queue."
  ([db queue]
   (dequeue-msg-stmt db queue (* 10 1000)))
  ([db queue timeout-msecs]
   (doto (.prepareCall (:connection db) dequeue-msg-sql)
     (.registerOutParameter 3 java.sql.Types/VARCHAR)
     (.setString 1 queue)
     (.setLong 2 (/ (or timeout-msecs 0) 1000)))))

(defn execute-dequeue-msg!
  "Executes the given prepared dequeue statement, returning the
  message body as a string or throwing a `java.sql.SQLException` on
  error.

  Does _not_ close the statement."
  [stmt]
  (.execute stmt)
  (.getString stmt 3))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defprotocol QueueStore
  (queue-exists? [this queue])
  (dequeue-loop [this queue f]))

(defrecord IFSQueueStore [database]
  component/Lifecycle
  (start [this] this)
  (stop [this] this)

  QueueStore
  (queue-exists? [this queue]
    (-queue-exists? database queue))
  (dequeue-loop [this queue f]
    (retry-loop
     (fn []
       (let [c (doto (jdbc/get-connection database)
                 (.setAutoCommit false))
             s (dequeue-msg-stmt {:connection c} queue)]
         [c s]))
     (fn [c s]
       (try
         (when-let [m (execute-dequeue-msg! s)]
           (f m)
           (.commit c)
           [:ok])
         (catch java.sql.SQLException e
           (try (.rollback c) (catch Exception _ nil))
           (if (dequeue-timeout? e)
             [:ok]
             [:fail e]))
         (catch Exception e
           (try (.rollback c) (catch Exception _ nil))
           [:retry e])))
     (fn [e]
       (and (instance? java.sql.SQLException e)
            (contains? #{connection-lost session-killed}
                       (.getErrorCode e)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-queue-store []
  (component/using
   (map->IFSQueueStore {})
   [:database]))
