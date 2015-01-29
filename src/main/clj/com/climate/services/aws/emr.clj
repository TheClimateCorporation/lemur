(ns com.climate.services.aws.emr
  (:use
    com.climate.services.aws.common)
  (:require
    [lemur.util :as util]
    [clojure.tools.logging :as log]
    [clojure.string :as s]
    [com.climate.services.aws.ec2 :as ec2])
  (:import
    java.io.File
    com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient
    com.amazonaws.services.elasticmapreduce.util.StepFactory
    com.amazonaws.auth.BasicAWSCredentials
    [com.amazonaws.services.elasticmapreduce.model
      ActionOnFailure
      AddJobFlowStepsRequest
      BootstrapActionConfig
      DescribeJobFlowsRequest
      HadoopJarStepConfig
      InstanceGroupConfig
      JobFlowInstancesConfig
      KeyValue
      PlacementType
      RunJobFlowRequest
      ScriptBootstrapActionConfig
      StepConfig
      StepDetail
      Tag
      TerminateJobFlowsRequest]))

; TODO All functions that use this dynamic var should have an additional fn
;      signature where the object can be passed in explicitly
(def ^{:dynamic true} *emr* nil)

(defn emr-client [aws-creds]
  (AmazonElasticMapReduceClient. aws-creds))

(defn emr
  [creds]
  (aws emr-client creds))

(def jobflow-tag "aws:elasticmapreduce:job-flow-id")

(defn instances
  "Return a seq of Instance(s) for the given jobflow-ids which is a collection or
   a single jobflow-id String."
  [jobflows]
  (ec2/instances-tagged jobflow-tag (util/collectify jobflows)))

(defn flow-id [flow] (if flow (.getJobFlowId flow)))

(defn flow-name [flow] (if flow (.getName flow)))

(defn flow-for-name [name]
  (let [req (.withJobFlowStates (DescribeJobFlowsRequest.)
                                (into-array ["RUNNING" "WAITING" "STARTING"]))
        result (.describeJobFlows *emr* req)
        flows (.getJobFlows result)
        flows (filter #(= (flow-name %) name) flows)]
    (first flows)))

(defn job-flow-detail
  "Get the JobFlowDetail."
  [jobflow-id]
  (->> [jobflow-id]
       (DescribeJobFlowsRequest.)
       (.describeJobFlows *emr*)
       .getJobFlows
       first))

(defn jobflow-status
  "Return the state of the specified cluster."
  [jobflow-id]
  (.. (job-flow-detail jobflow-id) getExecutionStatusDetail getState))

(defn jobflow-master
  "Return the master public DNS name."
  [jobflow-id]
  (.. (job-flow-detail jobflow-id) getInstances getMasterPublicDnsName))

(defn steps-for-jobflow [jobflow-id]
  "Get the StepDetail objects for the given jobflow."
  (-> jobflow-id
      job-flow-detail
      (#(if % (.getSteps %)))))

(defn step-detail
  "Returns the most recent StepDetail matching step-name."
  [jobflow-id step-name]
  (log/debugf "Steps for %s, looking for %s in %s"
              jobflow-id step-name (steps-for-jobflow jobflow-id))
  (first (filter #(->> % .getStepConfig .getName (= step-name))
                 (steps-for-jobflow jobflow-id))))

(defn step-status
  "Get the status string for this step. One of PENDING, RUNNING,
  CONTINUE, COMPLETED, CANCELLED, FAILED, INTERRUPTED"
  ([^StepDetail sd]
    (when sd (.getState (.getExecutionStatusDetail sd))))
  ([jobflow-id step-name]
    (step-status (step-detail jobflow-id step-name))))

(defn- wait-on-step*
  [end-time jobflow-id step-name test-interval-seconds]
  (let [sd (step-detail jobflow-id step-name)]
    (condp contains? (step-status sd)
      #{nil}
        {:success false}
      #{"COMPLETED"}
        {:success true}
      #{"CONTINUE" "CANCELLED" "FAILED" "INTERRUPTED"}
        {:success false
         :step-status-detail (.getExecutionStatusDetail sd)
         :job-status-detail (.getExecutionStatusDetail (job-flow-detail jobflow-id))}
      #{"PENDING" "RUNNING"}
        (if (< (System/currentTimeMillis) end-time)
          (do
            (Thread/sleep (* test-interval-seconds 1000))
            (recur end-time jobflow-id step-name test-interval-seconds))
          {:success false :timeout true}))))

(defn wait-on-step
  "Wait for the named step to complete or fail. EMR status will be checked
  every test-interval-seconds (default 60). If the jobflow or step is not found, it
  will return nil.  If the job step is still running past the timeout seconds, it
  will also return nil.
  If you make the test-interval-seconds too small, you can reach the AWS limit
  for emr status requests."
  ([jobflow-id step-name timeout-seconds]
    (wait-on-step jobflow-id step-name timeout-seconds 60))
  ([jobflow-id step-name timeout-seconds test-interval-seconds]
    (wait-on-step* (+ (System/currentTimeMillis) (* timeout-seconds 1000))
      jobflow-id step-name test-interval-seconds)))

(defn wait-until-provisioned
  "Wait for the given jobflow-id to finish startup action."
  [jobflow-id]
  (let [retry-pred #(not (contains? #{"STARTING"} %))]
    (util/fixed-retry-until #(jobflow-status jobflow-id) 20 30 retry-pred)))

(defn wait-until-accepted
  "Wait for the given jobflow-id to report a master node."
  [jobflow-id]
  (util/fixed-retry-until #(jobflow-master jobflow-id) 20 30 identity))

(defn wait-until-ready
  "Wait (upto 30 minutes) for the given jobflow-id to finish startup and bootstrap actions."
  [jobflow-id]
  (let [retry-pred #(not (contains? #{"STARTING" "BOOTSTRAPPING"} %))]
    (util/fixed-retry-until #(jobflow-status jobflow-id) 30 60 retry-pred)))

(defn wait-and-exit [jobflow-id step-name timeout-seconds]
  (System/exit
    (let [{:keys [success timeout step-status-detail job-status-detail]}
        (wait-on-step jobflow-id step-name timeout-seconds)]
    (cond
      success
        (do
          (log/warn "Job completed at" (util/time-str))
          0)
      (and (not success) timeout)
        (do
          (log/error "Timeout has been reached, but the job is still running. It may still complete sucessfully.")
          -1)
      (and (not success) step-status-detail)
        (do
          (log/error (format "Job failed. (step-state=%s, step-reason=%s, jobflow-reason=%s)"
                             (.getState step-status-detail)
                             (.getLastStateChangeReason step-status-detail)
                             (.getLastStateChangeReason job-status-detail)))
          1)
      :else
        (do
          (log/error "Job failed. Jobflow id or step not found.")
          2)))))

(defn instance-group-config
  [group instance-type number]
  (InstanceGroupConfig. group instance-type (Integer. number)))

(defn instances-config [{:keys [master-type slave-type num-instances keypair keep-alive
                                availability-zone spot-task-type spot-task-bid
                                spot-task-num hadoop-version subnet-id]
                         :or {master-type "m1.large"
                              slave-type "m1.large"
                              num-instances 3
                              keep-alive false}}]
  (let [master-config (instance-group-config "MASTER" master-type 1)
        core-config (when (> num-instances 1)
                      (instance-group-config "CORE" slave-type (- num-instances 1)))
        task-config (when spot-task-num
                      (doto (instance-group-config "TASK" spot-task-type spot-task-num)
                        (.setBidPrice (str spot-task-bid))
                        (.setMarket "SPOT")))
        jf (doto (JobFlowInstancesConfig.)
             (.setKeepJobFlowAliveWhenNoSteps keep-alive)
             (.setInstanceGroups (filter identity [master-config core-config task-config]))
             (.setEc2KeyName keypair)
             (.setEc2SubnetId subnet-id))]
    (when hadoop-version
      (.setHadoopVersion jf hadoop-version))
    (when availability-zone
      (.setPlacement jf (PlacementType. availability-zone)))
    jf))

;http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuide/Bootstrap.html
(defn bootstrap
  ([name path args]
    (BootstrapActionConfig. name (ScriptBootstrapActionConfig. path args)))
  ([name path arg1 & [more-args]]
    (bootstrap name path (cons arg1 more-args)))
  ([name path]
    (bootstrap name path []))
  ([path]
    (bootstrap "Custom Config" path)))

(defn- kv-props
  "Convert a Map into a Collection of EMR KeyValue objects"
  [m]
  (map (fn [[k v]] (KeyValue. k v)) m))

(defn debug-step-config
  "Create a job step to enable EMR debugging for all subsequent steps. Requires SimpleDB account."
  []
  (doto (StepConfig.)
    (.setName "Enable Debugging")
    (.setActionOnFailure (str ActionOnFailure/TERMINATE_JOB_FLOW))
    (.setHadoopJarStep (.newEnableDebuggingStep (StepFactory.)))))

(defn terminate-flow-id
  ([jobflow-id]
     (terminate-flow-id jobflow-id *emr*))
  ([jobflow-id emr]
     (.terminateJobFlows emr
                         (TerminateJobFlowsRequest. (java.util.ArrayList. [jobflow-id])))))

(defn step-config [name alive? jar-path main-class cli-args & {:keys [action-on-failure properties]}]
  "Create a step to be submitted to EMR.
  Inputs:
    name - An symbolic name for this step
    alive? - Whether or not the Step should be run as keep-alive (i.e. don't terminate the cluster)
    jar-path is the hadoop job jar, usually an s3:// path.
    main-class - A String containing the fully qualified class name with a main function or nil
    cli-args is a collection of Strings that are passed as args to main-class (can be nil).
    action-on-failure is a String or enum com.amazonaws.services.elasticmapreduce.model.ActionOnFailure.
    properties is a map of Java properties that are set when the step runs."
  (let [sc (StepConfig. name
                        (doto
                          (HadoopJarStepConfig.)
                          (.setJar jar-path)
                          (.setMainClass main-class)
                          (.setArgs (vec cli-args)) ;collection of strings
                          (.setProperties (kv-props properties))))]
    (.setActionOnFailure sc (str (or action-on-failure
                                     (and alive? ActionOnFailure/CANCEL_AND_WAIT)
                                     ActionOnFailure/TERMINATE_JOB_FLOW)))
    sc))

(defn add-steps
  "Add a step to a running jobflow. Steps is a seq of StepConfig objects.
  Use (step-config) to create StepConfig objects."
  [jobflow-id steps]
  (let [steps-array (to-array steps)]
    (.addJobFlowSteps *emr* (AddJobFlowStepsRequest. jobflow-id steps))))

(defn- parse-tags
  "tags is of format \"key0:value0,key1:value1,key2:value2\".
  Parse it into a sequence of Tag objects."
  [tags]
  (letfn [(->tag [[k v]] (Tag. k v))]
    (->> (s/split tags #",")
         (map s/trim)
         (map #(s/split % #"="))
         (map ->tag))))

(defn start-job-flow [name steps {:keys [log-uri bootstrap-actions ami-version
                                         supported-products visible-to-all-users
                                         job-flow-role service-role tags]
                                  :or {bootstrap-actions []
                                       supported-products []
                                       visible-to-all-users false}
                                  :as all}]
  (log/info (str "Starting JobFlow " all))
  (let [instances (instances-config all)
        request (doto (RunJobFlowRequest.)
                      (.setName name)
                      (.setLogUri log-uri) ; can be nil (i.e. no logs)
                      (.setInstances instances)
                      (.setTags (parse-tags tags)) ; can be an empty list
                      (.setAmiVersion ami-version)
                      (.setServiceRole service-role)
                      (.setJobFlowRole job-flow-role)
                      (.setSupportedProducts supported-products)
                      (.setBootstrapActions bootstrap-actions)
                      (.setVisibleToAllUsers visible-to-all-users)
                      (.setSteps steps))]
    (.getJobFlowId (.runJobFlow *emr* request))))


(defn- parse-spot-task-bid
  "type is an instance-type string.  arg is a percent (\"25%\") or fixed price (\"0.85\")
  as a string. E.g. (parse-spot-task-bid \"m1.large\" \"25%\").  A percent will be the percent
  of the difference between the reserve price (0%) and the demand price (100%).  Can be > 100%."
  [type arg]
  (format "%.3f"
    (if-not (re-find #"%" arg)
      (Double. arg)
      (let [pct (->> arg
                  (re-find #"(\d+)%")
                  second
                  Double.
                  (#(/ % 100.0)))
            reserve (ec2/reserve-price type)
            demand (ec2/demand-price type)]
        (when-not (and reserve demand)
          (throw (RuntimeException. (str "Could not find pricing for " type ". Perhaps "
                                         "the data has not been updated for this instance type."))))
        (+ reserve (* (- demand reserve) pct))))))

(defn parse-spot-task-group
  [spot-task-group]
  (when spot-task-group
    (let [[spot-task-type spot-task-bid spot-task-num]
            (s/split spot-task-group #",")
          spot-task-num (Integer. spot-task-num)
          spot-task-bid (parse-spot-task-bid spot-task-type spot-task-bid)]
      [spot-task-type spot-task-bid spot-task-num])))
