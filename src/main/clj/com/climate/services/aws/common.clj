(ns com.climate.services.aws.common
  (:require
    [clojure.string :as s]
    [com.climate.io :as ccio]
    [com.climate.shell :as sh]
    [clojure.data.json :as json]
    [clojure.tools.logging :as log]
    [clojure.java.io :as io])
  (:import
    java.io.IOException
    [com.amazonaws.auth BasicAWSCredentials]))

(defn aws
  ([client-f the-creds]
   (aws client-f (:access_id the-creds) (:private_key the-creds)))
  ([client-f access-key secret-key]
   (client-f (BasicAWSCredentials. access-key secret-key))))

(defn load-credentials
  "Loads a credentials file.

  path is a String or File. Immediately returns nil if path is nil, or is not an
  existing file.

  If the filename ends in '.json', then the file is expected to be in JSON format,
  same as that expected by the Ruby elastic-mapreduce client. E.g.:

     {\"access_id\": \"0BCWOFMDV82HJBSHFAKE\",
      \"private_key\": \"Sample/GudsbGjjJuz0gf6asdgvxasdasdv521gd\",
      \"keypair\": \"my-keypair\",
      ...}

  Otherwise, the format is that expected by AWS CloudSearch and other AWS tools.
  E.g. a properties file with

    accessKey=AKIAIOSFODNN7EXAMPLE
    secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY

  Returns a Map with keys [:access_id :private_key :keypair]"
  [path]
  (log/debug "Attempting to load aws credentials:" path)
  (let [f (io/file path)
        result (when (and f (.isFile f))
                 (if (->> f .getName (re-find #"\.json$"))
                   (-> f slurp json/read-json)
                   (let [props (into {} (ccio/load-properties f))]
                     (log/debug "aws credentials from properties"
                                (ccio/load-properties f)
                                " => " props)
                     ; AWSSecretKey/AWSAccessKeyId -- this is the format used by AWS CloudWatch
                     {:access_id (or (get props "accessKey") (get props "AWSSecretKey"))
                      :private_key (or (get props "secretKey") (get props "AWSAccessKeyId"))
                      :keypair (get props "keypair")})))]
    (when result (log/debug "Loaded aws-credentials" result))
    result))

(defn aws-cf-env []
  (System/getenv "AWS_CREDENTIAL_FILE"))

(defn aws-credential-discovery
  "Attempt to discover the path of your AWS credential file.
     - Look for a path identified by environment variable AWS_CREDENTIAL_FILE
     - Look in the PWD (present working dir) for credentials.json
     - Look for credentials.json by search-path, or at top of dir search-path
     - Look for `which elasic-mapreduce`/credentials.json.

  For details on the expected file format, see the doc on 'load-credentials'

  throws RuntimeException is the aws creds can not be found and loaded."
  [& [search-path]]
  ; TODO support the s3cmd file format (~/.s3cfg)
  (or
    (log/debug "Looking for aws creds in" (aws-cf-env) "or PWD.")
    (load-credentials (io/file (aws-cf-env)))
    (when search-path
      (log/debug "Looking for aws creds in" search-path)
      (load-credentials (io/file search-path))
      (log/debug "Looking for aws creds at " (str search-path "/credentials.json"))
      (load-credentials (io/file search-path "credentials.json")))
    (log/debug "Looking for aws creds in elastic-mapreduce install directory")
    (let [result (try (sh/sh "which" "elastic-mapreduce")
                      (catch IOException iox
                        (log/debug iox "Not able to find elastic-mapreduce cli.")))]
      (when result
        (log/debug "aws creds - found elastic-mapreduce at" (s/trim (:out result)))
        (-> (:out result)
          s/trim
          ccio/dirname
          (io/file "credentials.json")
          load-credentials)))
    (throw (RuntimeException.
      (if (aws-cf-env)
        (format "Can not find or read the aws credential file identified by AWS_CREDENTIAL_FILE='%s'" aws-cf-env)
        (str "Can not find or read the aws credential file. Try one of "
             "a) setting env variable AWS_CREDENTIAL_FILE, "
             "b) installing elastic-mapreduce cli and make sure it is configured and in your PATH, "
             "c) Creating a credentials.json file in the PWD, where "
             "credentials.json contains {\"access_id\": \"EXAMPLE\", \"private_key\": \"Example\"}."))))))
