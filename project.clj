(defproject lemur "1.2.0"

  :description "Lemur is a tool to launch hadoop jobs locally or on EMR
                based on a configuration file, referred to as a jobdef."

  :jvm-opts
    ~(if (.exists (java.io.File. "resources/log4j.properties"))
       ["-Dlog4j.configuration=file:resources/log4j.properties"]
       [])

  :source-path "src/main/clj"
  :test-path "src/test/clj"

  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/data.json "0.1.2"]

                 ; aws-java-sdk-1.3.3 does not specify the correct httpclient, so we do it explicitly
                 [org.apache.httpcomponents/httpclient "4.1.1"]
                 [com.amazonaws/aws-java-sdk "1.3.24"
                  :exclusions [javax.mail/mail org.apache.httpcomponents/httpclient]]

                 ; TODO these two are only to support hipchat-- isolate that functionality, so these libs can be optional
                 [clj-http "0.1.3"]
                 [org.yaml/snakeyaml "1.7"]

                 ; TODO we should be able to consolidate on one or the other of these:
                 [com.google.guava/guava "10.0.1"]
                 [commons-io/commons-io "1.4"]

                 ; Other
                 [log4j/log4j "1.2.16"]]

  :dev-dependencies [[robert/hooke "1.1.2"] ;for leiningen test-selectors
                     [org.clojure/tools.trace "0.7.1"]
                     [midje "1.3.1"]
                     [lein-midje "1.0.8"]
                     [com.offbytwo.iclojure/iclojure "1.1.0"]
                     [clojure-source "1.3.0"]]

  :test-selectors {:default (fn [v] (not (or (:integration v) (:manual v))))
                   :integration :integration
                   :manual :manual
                   :all (fn [v] (not (:manual v)))}

  :repl-init lemur.repl
  :main ^:skip-aot lemur.repl

  :run-aliases {:lemur lemur.core}

  ; Launch irepl:
  ;java -cp lib/*:lib/dev/* com.offbytwo.iclojure.Main

  :aot [lemur.core])
