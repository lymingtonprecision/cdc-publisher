(ns cdc-publisher.retry
  "Encapsulates the concept of a retry-able and recoverable process
  loop."
  (:require [clojure.core.async :as async :refer [go-loop]]
            [clojure.tools.logging :as log])
  (:import [java.lang.Math]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defaults

(defn default-backoff
  "The default retry back-off calculation; returns the cube of the
  current attempt rounded up to the nearest 5 seconds."
  [attempt max-retries]
  (if (zero? attempt)
    0
    (max 5 (* 5 (Math/round (/ (Math/pow attempt 3) 5))))))

(def default-retries 10)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-counter
  "Returns a new retry counter, with either the default number of
  retries and back-off fn or the given overrides."
  ([]
   (new-counter default-retries))
  ([max-retries]
   (new-counter max-retries default-backoff))
  ([max-retries backoff-fn]
   {:retries (atom 0)
    :max-retries max-retries
    :backoff backoff-fn}))

(defn reset-counter!
  "Resets the given retry counter `c` to zero retries."
  [c]
  (reset! (:retries c) 0)
  c)

(defn retry?!
  "If the given retry counter has not yet reached its maximum number
  of retries, the retry count is incremented and then the current
  thread blocked for the calculated back-off number of seconds.

  If the retry count has been exceeded then the exception, `e`, is
  re-thrown."
  [c e]
  (let [attempt (swap! (:retries c) inc)
        delay ((:backoff c) attempt (:max-retries c))]
    (if (> attempt (:max-retries c))
      (do
        (log/error "could not recover (after" (dec attempt) "tries) from" (.getMessage e))
        (throw e))
      (do
        (log/warn "waiting" delay "seconds to retry after error:" (.getMessage e))
        (async/<!! (async/timeout (* delay 1000)))))))

(defn retry-loop
  "Creates a go block that repeatedly calls `f` in a loop, retrying in
  the event of any errors.

  `f` should be a function that returns a one of the following variants:

  * `[:ok]`, the function was successful and should be repeated.
  * `[:stop]`, the function was successful but should not be repeated.
  * `[:retry error], a recoverable error was encountered and the call
    should be retried.`
  * `[:fail error]`, a non-recoverable error was encountered and
    processing should stop.

  An optional `prep-fn` may be provided that is invoked prior to
  running the loop inside which `f` is called. It should take no
  arguments and return a sequence of arguments to be passed to each
  invocation of `f` (e.g. established database connections, etc.)

  `recoverable?` is an optional fn that will be passed any uncaught
  exceptions arising from `f` or re-thrown as a result of it
  failing. If it returns truthy then processing is retried, with
  `prep-fn` being re-called before starting another loop of calls to
  `f`.

  The number of retries is dictated by the `retry-counter`, which
  defaults to 10 attempts. To alter this behaviour, see the
  `new-counter` fn. The counter is reset when `f` returns `[:ok]`."
  ([f]
   (retry-loop nil f nil nil))
  ([prep-fn f]
   (retry-loop prep-fn f nil nil))
  ([prep-fn f recoverable?]
   (retry-loop prep-fn f recoverable? (new-counter)))
  ([prep-fn f recoverable? retry-counter]
   (let [closed? (atom false)
         prep-fn (or prep-fn (constantly nil))
         recoverable? (or recoverable? (constantly false))]
     (go-loop []
       (try
         (let [vars (prep-fn)]
           (loop []
             (let [[status e] (apply f vars)]
               (case status
                 :ok (reset-counter! retry-counter)
                 :stop (reset! closed? true)
                 :retry (retry?! retry-counter e)
                 :fail (throw e))
               (when-not @closed? (recur)))))
         (catch Exception e
           (if (recoverable? e)
             (when-not @closed? (retry?! retry-counter e))
             (do
               (log/error "unhandled exception" e)
               (throw e)))))
       (when-not @closed? (recur)))
     (reify
       java.io.Closeable
       (close [this]
         (reset! closed? true))))))
