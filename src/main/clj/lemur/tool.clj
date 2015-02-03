(ns lemur.tool
  (:require
   [clojure.java.io :as io]
   [com.climate.services.aws
    [common :as awscommon]
    [ec2 :as ec2]
    [s3 :as s3]
    [emr :as emr]]
   [lemur [core :as l]
    [command-line :as cli]])
  (:gen-class))

(defn -main
  "Run lemur help."
  [& [command & args]]
  (try
    (cli/add-command-spec* l/context cli/init-command-spec)
    (let [[profiles remaining] (split-with #(.startsWith % ":") args)
          aws-creds (awscommon/aws-credential-discovery)
          jobdef-path (first remaining)]
      (l/add-profiles profiles)
      (l/context-set :jobdef-path jobdef-path)
      (l/context-set :raw-args (rest remaining))
      (l/context-set :command command)
      ;; TODO lazy load credentials instead
      (binding [s3/*s3* (s3/s3 aws-creds)
                emr/*emr* (emr/emr aws-creds)
                ec2/*ec2* (ec2/ec2 aws-creds)]
        (case command
          ("run" "start" "dry-run" "local" "submit")
          (l/execute-jobdef jobdef-path)
          "display-types"
          (l/display-types)
          "spot-price-history"
          (l/spot-price-history)
          "version"
          (cli/quit :msg (str "Lemur " (System/getenv "LEMUR_VERSION")))
          "formatted-help"
          (l/execute-jobdef jobdef-path)
          "help"
          (if jobdef-path
            (l/execute-jobdef jobdef-path)
            (cli/quit :msg (slurp (io/resource "help.txt"))))
          (cli/quit :msg (if command (str "Unrecognized lemur command: " command))
                    :cmdspec (l/context-get :command-spec) :exit-code 1))))
    (catch clojure.lang.ExceptionInfo e
      (cli/quit-by-error e)))
  (cli/quit))
