(ns cdc-publisher.core
  (:require [cheshire.core :as cheshire]
            [cdc-util.format]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn dml->msg
  "Given a DML message string returns a map of the `{:key ... :value ...}`
  that should be submitted to Kafka when sending it the DML as a message."
  [dml]
  (let [key (->> (get (cheshire/parse-string dml false) "id")
                 seq
                 (sort-by first)
                 flatten
                 cheshire/generate-string)]
    {:key key :value dml}))
