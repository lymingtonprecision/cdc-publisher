(ns cdc-publisher.test-generators
  (:require [clojure.test.check.generators :as gen]
            [cdc-util.test-generators :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(def gen-dml-map
  (gen/hash-map
   :id (gen/not-empty (gen/map gen/keyword (gen/one-of
                                            [gen-non-empty-string
                                             gen/pos-int
                                             gen/neg-int
                                             gen/boolean
                                             gen-rand-time])))
   :type (gen/elements [:insert :update :delete])
   :table gen-non-empty-string
   :data (gen/not-empty (gen/map gen/keyword gen/simple-type-printable))
   :info (gen/hash-map
          :user gen-non-empty-string
          :timestamp gen-rand-time)))
