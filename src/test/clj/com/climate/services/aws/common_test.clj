(ns com.climate.services.aws.common-test
  (:use
    com.climate.services.aws.common
    clojure.test
    midje.sweet)
  (:require
    [com.climate.shell :as sh]))

(let [expected
        {:access_id "SAMPLE_ID"
         :private_key "sample-secret-key"
         :keypair "optional-keypair"}
      expected2
        {:access_id "SAMPLE_ID2"
         :private_key "sample-secret-key2"
         :keypair "optional-keypair2"}]

  (deftest test-load-credentials
    (fact
      (load-credentials nil) => nil
      (load-credentials "src/test/resources") => nil
      (load-credentials "does-not-exist") => nil
      (load-credentials "src/test/resources/sample-credentials.json") => expected
      (load-credentials "src/test/resources/sample-aws-cred-file.properties") => expected))

  (deftest test-aws-credential-discovery
    (fact
      (aws-credential-discovery) => expected
      (provided (aws-cf-env) => "src/test/resources/sample-credentials.json")

      (aws-credential-discovery "src/test/resources/sample-credentials.json") => expected
      (provided (aws-cf-env) => nil)

      (aws-credential-discovery "src/test/resources/subdir") => expected2
      (provided (aws-cf-env) => nil)

      ; no test provided for PWD/credentials.json

      (aws-credential-discovery) => expected2
      (provided
        (aws-cf-env) => nil
        (sh/sh "which" "elastic-mapreduce") => {:out "src/test/resources/subdir/foo"})

      ; a failure test
      (aws-credential-discovery) => (throws RuntimeException #"Can not find")
      (provided
        (aws-cf-env) => nil
        (sh/sh "which" "elastic-mapreduce") => nil)))

  )
