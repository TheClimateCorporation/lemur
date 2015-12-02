(defproject lemur "1.4.5"

  :description "Lemur is a tool to launch hadoop jobs locally or on EMR
                based on a configuration file, referred to as a jobdef."

  :jvm-opts
    ~(if (.exists (java.io.File. "resources/log4j.properties"))
       ["-Dlog4j.configuration=file:resources/log4j.properties"]
       [])

  :source-paths ["src/main/clj"]
  :test-paths   ["src/test/clj"]

  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/data.json "0.1.2"]
                 [bultitude "0.2.0"]

                 [com.amazonaws/aws-java-sdk-emr "1.10.37"]
                 [com.amazonaws/aws-java-sdk-s3 "1.10.37"]
                 [com.amazonaws/aws-java-sdk-ec2 "1.10.37"]
                 [commons-codec "1.10"]
                 [org.apache.httpcomponents/httpclient "4.4.1"]

                 ; TODO these two are only to support hipchat-- isolate that functionality, so these libs can be optional
                 [clj-http "0.1.3" :exclusions [httpclient]]
                 [org.yaml/snakeyaml "1.7"]

                 ; TODO we should be able to consolidate on one or the other of these:
                 [com.google.guava/guava "10.0.1"]
                 [commons-io/commons-io "1.4"]

                 ; Other
                 [log4j/log4j "1.2.16"]]

  :plugins [[lein-libdir "0.1.0"]]
  :libdir-path "lib"

  :profiles {:dev {:plugins [[lein-midje "2.0.4"]]
                   :dependencies [[midje "1.4.0"]
                                  [org.clojure/tools.trace "0.7.3"]]}
             :1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}}

  :repl-init lemur.repl
  :main ^:skip-aot lemur.repl
  :min-lein-version "2.0.0"

  :run-aliases {:lemur lemur.core}

  :test-selectors {:default (fn [v] (not (or (:integration v) (:manual v))))
                   :integration :integration
                   :manual :manual
                   :all (fn [v] (not (:manual v)))}

  :aot [lemur.core]
  )
