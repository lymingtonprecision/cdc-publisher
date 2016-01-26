(ns cdc-publisher.components.jms-publisher
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [cdc-util.schema.oracle-refs :as oracle-refs]
            [cdc-publisher.core :refer [dml->msg]]
            [cdc-publisher.protocols.queue :as queue]
            [cdc-publisher.protocols.ccd-store :as ccd-store :refer [CCDStore]])
  (:import javax.jms.Session
           javax.jms.ExceptionListener
           javax.jms.MessageListener
           oracle.jms.AQjmsFactory
           oracle.sql.ORADataFactory))

(def ^:dynamic *log-error-rate-mins* 10)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility records

(defrecord QueueMessage [format data queued-at]
  oracle.sql.ORAData
  (toDatum [this c]
    nil))

(def queue-message
  "A simple stub for conversion of JMS messages to our custom queue message
  struct."
  ->QueueMessage)

(defrecord QueueConnection [db-spec]
  component/Lifecycle
  (start [this]
    (if (:connection this)
      this
      (let [f (get this :factory
                   (AQjmsFactory/getQueueConnectionFactory
                    (:db-server db-spec)
                    (:db-name db-spec)
                    (get db-spec :db-port 1521)
                    "thin"))
            c (doto (.createQueueConnection
                     f
                     (:db-user db-spec)
                     (:db-password db-spec))
                (.setExceptionListener
                 (reify javax.jms.ExceptionListener
                   (onException [this e]
                     (log/error e)
                     (throw e))))
                .start)
            s (.createQueueSession c false Session/AUTO_ACKNOWLEDGE)]
        (assoc this :factory f :connection c :session s))))
  (stop [this]
    (when-let [s (:session this)]
      (.close s))
    (when-let [c (:connection this)]
      (.close c))
    (dissoc this :session :connection)))

(def queue-connection
  "Encapsulates the object instances associated with a single JMS connection.

  `db-spec` must be a map containing the `:db-server`, `:db-name`, `:db-user`,
  and `:db-password` to use for establishing the connection.

  The created connection will automatically acknowledge received messages (if
  the receiver does not throw an error.)"
  ->QueueConnection)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(defn- now-millis
  "Returns the current system time in milliseconds"
  []
  (java.lang.System/currentTimeMillis))

(defn- minutes-ago
  "Returns the number of minutes that have elapsed since the time represented by
  the given a number of milliseconds since the start of the epoch."
  [millis]
  (int (/ (- (now-millis) millis) 1000 60)))

(defn- <mins
  "Returns true if `n`, a time specified in milliseconds, is less than the
  specified number of minutes ago (relative to current system time.)"
  [n mins]
  (some-> n minutes-ago (< mins)))

(defn read-clob
  "Returns a string representation of the data contained in an Oracle `CLOB`."
  [clob]
  (let [sb (StringBuilder.)]
    (with-open [r (.getCharacterStream clob)]
      (loop [c (.read r)]
        (if (neg? c)
          (str sb)
          (do
            (.append sb (char c))
            (recur (.read r))))))))

(def ora-data->queue-message
  "An `ORADataFactory` instance that converts dequeued JMS message payloads into
  `QueueMessage`s."
  (reify oracle.sql.ORADataFactory
    (create [this datum type]
      (apply queue-message (.getAttributes datum)))))

(defn extract-msg-body
  "Returns the raw message data string from a dequeued `QueueMessage`."
  [message]
  (-> message .getAdtPayload :data read-clob))

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

(defn enqueue-jms-message
  "Attempts to parse and enqueue the given JMS message to `queue` on `dst`.

  Raises `queue/*malformed-message-error` if message cannot be converted to a
  DML JSON map.

  Retries the message enqueue 10 times in the event of a
  `queue/*retriable-enqueue-error`."
  [dst queue message]
  (let [message-body (extract-msg-body message)]
    (try
      (let [parsed-message (dml->msg message-body)]
        (binding [queue/*retriable-enqueue-error*
                  (fn [& args]
                    (queue/reset! dst)
                    [::retry args])]
          (loop [attempt 0]
            (when-let [r (queue/enqueue! dst queue parsed-message)]
              (if (and (sequential? r) (= ::retry (first r)))
                (if (= attempt 10)
                  (apply queue/*enqueue-error* (second r))
                  (recur (inc attempt))))))))
      (catch com.fasterxml.jackson.core.JsonParseException e
        (queue/*malformed-message-error*
         (str "malformed message received from " queue)
         {:queue queue
          :message message}))
      (catch Exception e
        (log/error (str "error processing message from " queue) e)
        (throw e)))))

(defn queue-publisher
  "Returns a JMS `MessageConsumer` listening for messages on `queue` and posting
  them to `dst`.

  Received messages are assumed to be LPE DML formatted `QueueMessage`s and will
  be parsed as such. Any resulting parse failures will be logged and no
  acknowledgment of the message made (such that it will be repeatedly received
  by the listener, blocking the queue, until it can be processed successfully.)"
  [{s :session :as c} queue dst]
  (log/debug (str "creating publisher for " queue))
  (let [[schema queue-name] (rest (re-matches oracle-refs/queue-ref-regex queue))
        q (.getQueue s schema queue-name)
        last-dequeue-fail (atom nil)
        eh (fn [msg {:keys [queue] :as info}]
             (when-not (<mins @last-dequeue-fail *log-error-rate-mins*)
               (reset! last-dequeue-fail (now-millis))
               (log/error msg))
             (queue/*malformed-message-error* msg info))
        l (reify
            MessageListener
            (onMessage [this message]
              (binding [queue/*malformed-message-error* eh]
                (enqueue-jms-message dst queue message))))]
    (doto (.createReceiver s q nil ora-data->queue-message)
      (.setMessageListener l))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord JmsPublisher [db-spec ccd-store dst]
  component/Lifecycle
  (start [this]
    (if (some-> this :active? deref)
      this
      (let [active? (atom true)
            c (.start (get this :connection (queue-connection db-spec)))
            queues (atom {})
            queue-chan (async/chan 10 (map :queue))
            queue-enabler (async/go-loop []
                            (when-let [q (async/<! queue-chan)]
                              (swap! queues assoc q (queue-publisher c q dst))
                              (recur)))]
        (log/info "starting JMS publisher")
        (>!active-ccds ccd-store queue-chan)
        (assoc this
               :active? active?
               :connection c
               :chan queue-chan
               :queues queues))))
  (stop [this]
    (when-let [a (:active? this)]
      (log/info "stopping JMS publisher")
      (reset! a false))
    (when-let [ch (:chan this)]
      (async/close! ch))
    (when-let [qs (some-> this :queues deref)]
      (doseq [[q p] qs]
        (log/debug (str "stopping publication of " q))
        (.close p)))
    (when-let [c (:connection this)]
      (.stop c))
    (dissoc this :active? :chan :queues)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn jms-publisher [db-spec]
  (component/using
   (map->JmsPublisher {:db-spec db-spec})
   [:ccd-store :dst]))
