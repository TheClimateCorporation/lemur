(ns com.climate.services.aws.common
  (:import
    [com.amazonaws.auth DefaultAWSCredentialsProviderChain]))

(defn aws-credentials
  []
  (DefaultAWSCredentialsProviderChain.))
