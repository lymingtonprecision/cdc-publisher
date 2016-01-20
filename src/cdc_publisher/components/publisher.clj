(ns cdc-publisher.components.publisher
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [metrics.core :as metrics]
            [metrics.gauges :refer [gauge-fn]]
            [metrics.timers :refer [timer time!]]
            [cdc-publisher.core :refer [*metrics-group*]]
            [cdc-publisher.protocols.queue :as queue]
            [cdc-publisher.protocols.ccd-store :as ccd-store :refer [CCDStore]]))

(def ^:dynamic *log-error-rate-mins* 15)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Metrics

(defn queue-publisher-loop-timer []
  (timer [*metrics-group* "queue-publisher" "loop-time"]))

(def queue-publisher-queues-gauge
  [*metrics-group* "queue-publisher" "queues"])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(defn- minutes-ago [millis]
  (int (/ (- (java.lang.System/currentTimeMillis) millis) 1000 60)))

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

(defn queue-publisher-loop
  "Creates a loop that repeatedly iterates over `queues` passing the
  value of `(dequeue-fn queue)` to `publish-fn` for each `queue` in
  turn until `should-terminate?` returns true.

  Records total queue loop processing time to the
  `queue-publisher-loop-timer` metric.

  (Note: `publish-fn` is only called when `dequeue-fn` returns a
  non-nil value.)

  `queues` is the sequence of queues to process. It _may_ be an
  atom/future/ref in which case it will be `deref`erenced at the start
  of each iteration of the loop.

  `process-msg-from-queue` should accept a single argument—the queue
  extracted from `queues`—and process a single message from it. Any
  return value will be ignored.

  `should-terminate?` should be a `fn` taking zero arguments that
  returns truthy when the publication loop should be stopped.
  (Typically this will just be a simple wrapper around an `atom`
  that you're `reset!`ing to `false` when processing needs to
  stop.)

  Skips any malformed messages encountered during the loop, logging
  the error (no more often than once every `*log-error-rate-mins*`
  minutes) but otherwise continuing processing."
  [queues process-msg-from-queue should-terminate?]
  (let [dequeue-fails (atom {})]
    (binding [queue/*malformed-message-error*
              (fn [msg {:keys [queue] :as info}]
                (when-not (some-> (get @dequeue-fails queue)
                                  minutes-ago
                                  (< *log-error-rate-mins*))
                  (swap! dequeue-fails assoc queue (java.lang.System/currentTimeMillis))
                  (log/error "dequeue failed:" msg))
                (queue/*skip-message-dequeue*))]
      (loop []
        (time!
         (queue-publisher-loop-timer)
         (doseq [q (if (instance? clojure.lang.IDeref queues)
                     (deref queues)
                     queues)
                 :while (not (should-terminate?))]
           (process-msg-from-queue q)))
        (when-not (should-terminate?)
          (recur))))))

(defn queue-publisher-thread
  "Returns a user (non-daemon) thread executing a
  `queue-publisher-loop` with the provided `args`."
  [& args]
  (let [l (reify Runnable
            (run [this] (apply queue-publisher-loop args)))
        t (Thread. l)]
    (doto t
      (.setDaemon false)
      (.start))))

(defn dequeue-and-send!
  "Dequeues a message from `queue` on `src` on posts it to the same
  queue on `dst`.

  Retries enqueues (on a re-triable error) 10 times before logging
  them as failed and returning `nil` without committing the dequeue."
  [src dst queue]
  {:pre [(satisfies? queue/QueueReader src)
         (satisfies? queue/QueueWriter dst)]}
  (binding [queue/*enqueue-error*
            (fn [msg info]
              (log/error "enqueue failed:" msg))
            queue/*retriable-enqueue-error*
            (fn [& args]
              (queue/reset! dst)
              [::retry args])]
    (queue/dequeue-sync
     src queue
     (fn [msg]
       (loop [attempt 0]
         (when (pos? attempt) (log/debug "enqueue retry attempt" attempt))
         (when-let [r (queue/enqueue! dst queue msg)]
           (if (and (sequential? r) (= ::retry (first r)))
             (if (= attempt 10)
               (apply queue/*enqueue-error* (second r))
               (recur (inc attempt))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord Publisher [ccd-store src dst]
  component/Lifecycle
  (start [this]
    (if (and (:active? this) @(:active? this))
      this
      (let [active? (atom true)
            queues (atom [])
            queue-chan (async/chan 10 (map :queue))
            queue-enabler (async/go-loop []
                            (when-let [q (async/<! queue-chan)]
                              (log/info (str "adding " q " to the publication list"))
                              (swap! queues conj q)
                              (recur)))]
        (log/info "starting publisher")
        (gauge-fn queue-publisher-queues-gauge #(count @queues))
        (>!active-ccds ccd-store queue-chan)
        (assoc this
               :active? active?
               :chan queue-chan
               :thread (queue-publisher-thread
                        queues
                        (partial dequeue-and-send! src dst)
                        #(not @active?))))))
  (stop [this]
    (when-let [a (:active? this)]
      (log/info "stopping publisher")
      (reset! a false))
    (when-let [ch (:chan this)]
      (async/close! ch))
    (metrics/remove-metric queue-publisher-queues-gauge)
    (dissoc this :active? :chan :thread)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn publisher []
  (component/using
   (map->Publisher {})
   [:ccd-store :src :dst]))
