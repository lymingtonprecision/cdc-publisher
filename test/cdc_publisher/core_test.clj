(ns cdc-publisher.core-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [com.gfredericks.test.chuck.clojure-test :as chuck]

            [cheshire.core :as cheshire]

            [cdc-publisher.test-generators :refer :all]
            [cdc-publisher.core :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; dml->msg

(def ^:dynamic *dml->msg-test-count* 25)

(defspec dml->msg-uses-ordered-vector-of-id-as-key
  *dml->msg-test-count*
  (chuck/for-all
   [dml gen-dml-map]
   (let [dml-str (cheshire/generate-string dml)
         exp (cheshire/generate-string
              (vec (flatten (sort-by first (seq (:id dml))))))]
     (is (= exp (:key (dml->msg dml-str)))))))

(defspec dml->msg-takes-value-as-is
  *dml->msg-test-count*
  (chuck/for-all
   [dml gen-dml-map]
   (let [dml-str (cheshire/generate-string dml)]
     (is (= dml-str (:value (dml->msg dml-str)))))))
