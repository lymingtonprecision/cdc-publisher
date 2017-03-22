(def oracle-jar-version "11.1.0.7.0")

(defproject cdc-publisher "2.0.2"
  :description "LPE Change Data Capture publication service"
  :url "https://github.com/lymingtonprecision/cdc-publisher"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.442"]

                 ;; system
                 [com.stuartsierra/component "0.3.2"]
                 [environ "1.1.0"]
                 [clj-time "0.13.0"]

                 ;; logging
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-logging-config "1.9.12"]

                 ;; metrics
                 [metrics-clojure "2.9.0"]
                 [metrics-kafka-reporter/reporter-clj "0.1.0"]

                 ;; database
                 [org.clojure/java.jdbc "0.6.1"]
                 [yesql "0.5.3"]

                 ;; oracle jars
                 [oracle/ojdbc6 ~oracle-jar-version]
                 [oracle/aqapi ~oracle-jar-version]
                 [oracle/jta ~oracle-jar-version]
                 [oracle/xdb ~oracle-jar-version]
                 [oracle/jmscommon ~oracle-jar-version]
                 [oracle/xmlparserv2 ~oracle-jar-version]

                 ;; (de-)serialization
                 [cheshire "5.7.0"]

                 ;; kafka
                 [org.apache.kafka/kafka-clients "0.10.2.0"]

                 ;; utils
                 [lymingtonprecision/cdc-util "1.2.2"]]

  :main cdc-publisher.main
  :aot [cdc-publisher.main]

  :uberjar-name "cdc-publisher-standalone.jar"

  :plugins [[lein-localrepo "0.5.3"]]

  :profiles {:repl {:source-paths ["dev"]}
             :dev {:dependencies [[reloaded.repl "0.2.3"]
                                  [org.clojure/test.check "0.9.0"]
                                  [com.gfredericks/test.chuck "0.2.7"]]}}

  :repl-options {:init-ns user :init (reloaded.repl/init)}

  :aliases
  {"install-ojdbc"
   ["localrepo" "install" "oracle-jars/ojdbc6.jar" "oracle/ojdbc6" ~oracle-jar-version]
   "install-aqapi"
   ["localrepo" "install" "oracle-jars/aqapi.jar" "oracle/aqapi" ~oracle-jar-version]
   "install-jta"
   ["localrepo" "install" "oracle-jars/jta.jar" "oracle/jta" ~oracle-jar-version]
   "install-xdb"
   ["localrepo" "install" "oracle-jars/xdb.jar" "oracle/xdb" ~oracle-jar-version]
   "install-jmscommon"
   ["localrepo" "install" "oracle-jars/jmscommon.jar" "oracle/jmscommon" ~oracle-jar-version]
   "install-xmlparserv2"
   ["localrepo" "install" "oracle-jars/xmlparserv2.jar" "oracle/xmlparserv2" ~oracle-jar-version]}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version"
                   "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]])
