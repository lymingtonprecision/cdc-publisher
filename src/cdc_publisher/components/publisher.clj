(ns cdc-publisher.components.publisher
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [metrics.timers :refer [timer time!]]
            [cdc-publisher.core :refer [*metrics-group*]]
            [cdc-publisher.protocols.queue :as queue]
            [cdc-publisher.protocols.ccd-store :as ccd-store :refer [CCDStore]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Metrics

(defn queue-publisher-loop-timer []
  (timer [*metrics-group* "queue-publisher" "loop-time"]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

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
  "Returns an async thread that repeatedly loops over `queues` passing
  the value of `(dequeue-fn queue)` to `publish-fn` for each `queue`
  in turn until `should-terminate?` returns true.

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
  stop.)"
  [queues process-msg-from-queue should-terminate?]
  (async/thread
    (loop []
      (time!
       (queue-publisher-loop-timer)
       (doseq [q (if (instance? clojure.lang.IDeref queues)
                   (deref queues)
                   queues)
               :while (not (should-terminate?))]
         (process-msg-from-queue q)))
      (when-not (should-terminate?)
        (recur)))))

(defn dequeue-and-send!
  "Dequeues a message from `queue` on `src` on posts it to the same
  queue on `dst`."
  [src dst queue]
  {:pre [(satisfies? queue/QueueReader src)
         (satisfies? queue/QueueWriter dst)]}
  (queue/dequeue-sync src queue #(queue/enqueue! dst queue %)))

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
        (>!active-ccds ccd-store queue-chan)
        (assoc this
               :active? active?
               :chan queue-chan
               :thread (queue-publisher-loop
                        queues
                        (partial dequeue-and-send! src dst)
                        #(not @active?))))))
  (stop [this]
    (when-let [a (:active? this)]
      (log/info "stopping publisher")
      (reset! a false))
    (when-let [ch (:chan this)]
      (async/close! ch))
    (dissoc this :active? :chan :thread)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn publisher []
  (component/using
   (map->Publisher {})
   [:ccd-store :src :dst]))
