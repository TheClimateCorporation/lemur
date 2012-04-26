(ns com.climate.services.aws.core
  (:require
    [clojure.data.json :as json])
  (:import
    java.io.IOException
    [com.amazonaws.auth BasicAWSCredentials]))

(defn aws
  ([client-f the-creds]
   (aws client-f (:access_id the-creds) (:private_key the-creds)))
  ([client-f access-key secret-key]
   (client-f (BasicAWSCredentials. access-key secret-key))))

(defn load-credentials
  "Loads a credentials file. The file is JSON format, same as
   that expected by the Ruby elastic-mapreduce client. E.g.:
     {\"access_id\": \"0BCWOFMDV82HJBSHFAKE\",
      \"private_key\": \"Sample/GudsbGjjJuz0gf6asdgvxasdasdv521gd\",
      \"keypair\": \"my-keypair\",
      ...}"
  [path]
  (-> path slurp json/read-json))
