(ns com.climate.services.aws.emr-test
  (:use
    com.climate.services.aws.emr
    lemur.test
    clojure.test
    midje.sweet)
  (:require
    [com.climate.services.aws
     [s3 :as s3]
     [ec2 :as ec2]
     [common :as awscommon]])
  (:import
    [com.amazonaws.services.elasticmapreduce.model
     JobFlowDetail
     JobFlowExecutionStatusDetail]
    java.util.Date))

;; Some tests are labelled as :manual, rather than :integration, because
;; they're not just slow; they cost money by starting new EC2 instances.

(def bucket "lemur.unit.emr")
(def test-flow-name "com.climate.services.aws.emr-test")

(def aws-creds (awscommon/aws-credential-discovery))

(use-fixtures :once
  (fn [f]
    (binding [s3/*s3* (s3/s3 aws-creds)
              *emr* (emr aws-creds)]
      (f))))

;Starts a short-lived, 1 instance cluster. Has no bootstrap actions and no
;steps, so it will die right after starting up.
;NOTE: (setup) is not done as a fixture because we don't want it to run unless
;the test-selector :manual is used.  Test selectors don't impact fixtures.
(def setup
  (memoize
    (fn []
      (s3/create-bucket bucket)
      (s3/put-object-string bucket "scripts/wc.sh"
        "#!/bin/bash
        wc")
      (s3/put-object-string bucket "data/simple.txt"
        "line1 a b
        line2 c d")

      (println "These tests take a long time to run, and provide no feedback while running."
               "Try 'elastic-mapreduce --list' for an idea of where it's at.")
      (start-job-flow
        test-flow-name
        [(step-config
          "stream-step"
          false
          "/home/hadoop/contrib/streaming/hadoop-streaming.jar"
          nil
          ["-input" (format "s3://%s/data/simple.txt" bucket)
           "-output" "/out"
           "-mapper" (format "s3://%s/scripts/wc.sh" bucket)])]
        {:bootstrap-actions
           ; Only publicly available script, so we don't have to upload the others.
           [(bootstrap "Hadoop Config"
                       "s3://elasticmapreduce/bootstrap-actions/configure-hadoop"
                       ["-m" "mapred.map.tasks.speculative.execution=false"])]
         :log-uri (str "s3://" bucket)
         :keypair (:keypair aws-creds) ; the elastic-mapreduce credentials.json file as a keypair entry
         :ami-version "latest"
         :num-instances 2
         :master-type "m1.xlarge"
         :slave-type "m1.xlarge"
         :spot-task-type "m1.xlarge"
         :spot-task-bid "1.00"
         :spot-task-num 1
         ;too dangerous to use keep-alive in tests, so tests are limited-- no (emr/add-step) for example
         :keep-alive false}))))

; Specified as a fn rather than a test. This is a hack to force it to run before
; test-wait-on-step.  It will fail if the cluster has already COMPLETED.
(defn test-flow-for-name-and-types []
  (testing "emr/flow-for-name"
    (let [jf (setup)]
      (is (nil? (flow-for-name "non-existent")))
      (is= jf (flow-id (flow-for-name test-flow-name)))))
  (testing "non-default instance-types"
    (let [jf (setup)
          flow (flow-for-name test-flow-name)
          instances (.getInstances flow)]
      (is= "m1.xlarge" (.getMasterInstanceType instances))
      (is= "m1.xlarge" (.getSlaveInstanceType instances)))))

(deftest ^{:manual true} test-job-flow-detail
  (testing "emr/job-flow-detail"
    (let [jf (setup)
          detail (job-flow-detail jf)]
      (is (instance?
            com.amazonaws.services.elasticmapreduce.model.JobFlowDetail
            detail))
      (is= jf (flow-id detail))
      (is (instance? String (-> detail .getExecutionStatusDetail .getState))))))

(deftest ^{:manual true} test-steps-for-jobflow
  (testing "emr/steps-for-jobflow"
    (let [jf (setup)
          steps (steps-for-jobflow jf)]
      (is= 1 (count steps))
      (is= "stream-step" (-> steps first .getStepConfig .getName)))))

(deftest ^{:manual true} test-step-detail
  (testing "emr/step-detail"
    (let [jf (setup)
          sd (step-detail jf "stream-step")]
      (is (instance? com.amazonaws.services.elasticmapreduce.model.StepDetail sd))
      (is (nil? (step-detail jf "non-existant"))))))

(deftest ^{:manual true} test-step-status
  (testing "emr/step-status"
    (let [jf (setup)
          state (step-status jf "stream-step")]
      (is (contains? #{"PENDING" "RUNNING" "COMPLETED"} state)))))

(deftest ^{:manual true} test-wait-on-step
  (test-flow-for-name-and-types)  ;run the flow-for-name test before this one
  (testing "emr/wait-on-step"
    (let [jf (setup)
          start-time (System/currentTimeMillis)
          result (wait-on-step jf "stream-step" 600 20)
          dur-millis (- (System/currentTimeMillis) start-time)]
      (is (> dur-millis 2000)) ;will actually take several minutes -- mostly for EMR cluster startup
      (is (map? result))
      (is (:success result)))))

(deftest test-parse-spot-task-bid
  (let [parse-spot-task-bid (ns-resolve 'com.climate.services.aws.emr 'parse-spot-task-bid)]
    (fact (parse-spot-task-bid "m1.xlarge" "0.99") => "0.990")
    (fact (parse-spot-task-bid "m1.xlarge" "25%") => "1.250"
      (provided
        (ec2/reserve-price "m1.xlarge") => 1.0 :times 1
        (ec2/demand-price "m1.xlarge") => 2.0 :times 1))))

(deftest ^{:integration true} test-parse-spot-task-group
  (is (nil? (parse-spot-task-group nil)))
  (let [[spot-task-type spot-task-bid spot-task-num]
          (parse-spot-task-group "m1.xlarge,200%,20")
        spread (- (ec2/demand-price "m1.xlarge") (ec2/reserve-price "m1.xlarge"))
        expected-bid (+ (ec2/reserve-price "m1.xlarge") (* 2 spread))]
    (is= "m1.xlarge" spot-task-type)
    (is= 20 spot-task-num)
    (is= (format "%.3f" expected-bid) spot-task-bid)))
