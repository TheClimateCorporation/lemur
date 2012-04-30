(ns lemur.base
  (:use
    lemur.core
    [lemur.common :only [val-opts]]
    [com.climate.services.aws.common :only [aws-credential-discovery]]
    [lemur.bootstrap-actions :only [mk-hadoop-config]])
  (:require
    [clojure.tools.logging :as log]
    [lemur.util :as util]
    [com.climate.services.aws [s3 :as s3]]
    [com.climate.io :as ccio])
  (:import
    java.io.File
    java.util.GregorianCalendar
    java.util.TimeZone)
  (:gen-class))

(defn clear-base-uri
  "Deletes the contents of base-uri (works for local paths only, NOT s3)."
  [eopts]
  (println "clear-base-uri on" (:base-uri eopts))
  (when-not (dry-run?)
    (if (s3/s3path? (:base-uri eopts))
      (throw (UnsupportedOperationException. "Can not use clear-base-uri with an s3 path."))
      (ccio/delete-directory (File. (:base-uri eopts))))))

(add-hooks
  (local?) clear-base-uri)

(add-validators
  (val-opts :required [:keypair])
  (val-opts :required :word :app)
  (not (local?))
    (val-opts :required :bucket)
  (not (start?))
    (lfn [runtime-jar jar-src-path]
      (if-not (or runtime-jar jar-src-path)
        "Either --runtime-jar, --jar-src-path or lemur start must be specified.")))

(update-base
  (hash-map
    :ami-version "latest"

    :local
      {:run-path "${app}"
       :base-uri "/tmp/lemur/${run-path}"}

    :runtime (util/time-str "yyyy-MM-dd-HHmm")
    :uuid8 (util/uuid 8)
    :username (System/getProperty "user.name")
    :app "${jobdef-file}"
    :run-id "${app}/${runtime}-${uuid8}"
    :emr-name "${run-id}"
    :metajob-file true

    :master-instance-type "m1.large"
    :slave-instance-type "m1.large"
    :std-scripts-prefix "runs/${run-id}/emr-bootstrap"

    :run-path "${run-id}"
    :base-uri "s3://${bucket}/runs/${run-path}"
    :log-uri "${base-uri}/emr-logs"
    :data-uri "${base-uri}/data"
    :jar-uri "${base-uri}/jar"

    :bootstrap-action.100
      (fn [eopts]
        (when-let [hc (seq (mk-hadoop-config eopts))]
          ["Hadoop Config"
           "s3://elasticmapreduce/bootstrap-actions/configure-hadoop"
           hc]))

    ))
