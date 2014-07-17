;Copyright 2012 The Climate Corporation
;
;Licensed under the Apache License, Version 2.0 (the "License");
;you may not use this file except in compliance with the License.
;You may obtain a copy of the License at
;
;    http://www.apache.org/licenses/LICENSE-2.0
;
;Unless required by applicable law or agreed to in writing, software
;distributed under the License is distributed on an "AS IS" BASIS,
;WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;See the License for the specific language governing permissions and
;limitations under the License.

(ns lemur.core
  (:use
    lemur.command-line
    [lemur.bootstrap-actions :only [mk-bootstrap-actions
                                    hadoop-config-details
                                    bootstrap-actions-details]]
    [clojure.pprint :only [cl-format]])
  (:require
    clojure.set
    [lemur.evaluating-map]
    [com.climate.shell :as sh]
    [com.climate.io :as ccio]
    [clojure.java.io :as io]
    [clojure.string :as s]
    [clojure.tools.logging :as log]
    [lemur.util :as util]
    [com.climate.services.aws
     [common :as awscommon]
     [ec2 :as ec2]
     [s3 :as s3]
     [emr :as emr]])
  (:import
    com.amazonaws.services.elasticmapreduce.model.StepConfig
    [org.yaml.snakeyaml DumperOptions
                        DumperOptions$FlowStyle
                        DumperOptions$ScalarStyle])
  (:gen-class))

(comment

A FEW DESIGN PRINCIPLES

  1. Constructs are created and manipulated as Maps.  Evaluation and construction
     of model objects is deferred as late as possible.
  2. The job description (cluster settings and steps) are defined in a Clojure file, referred
     to as a jobdef file.  The sample-jobdef.clj is a good place to start.  It contains an
     exhaustive set of options with documentation comments.
  3. The config is built up from a few sources, in order of precedence-- command line, the jobdef
     file, and one or more bases.  The bases are clojure files with an update-base function and
     define the defaults.  See lemur.base for an example-- all jobdefs should use this
     base.  The jobdef names one or more bases using the (use-base) function, see
     sample-jobdef.clj for an example.
  4. Most option settings (Map values) can be literal values or they can be functions.
     See the doc-string for evaluating-map for a description of how a function value
     is evaluated.

GENERAL FLOW

main                      - entry point
calls execute-jobdef      - Loads and executes the jobdef file
the jobdef calls fire!    - triggers parsing on command line args
calls fire*               - interprets the structures (cluster, steps, options), and diagnostic output
calls launch              - take action (upload files, start cluster, etc)
)

(defonce context
  (atom {:jobdef-path nil     ;The path of the jobdef file being processed, if any
         :command nil         ;The lemur command, e.g. start, run, help, etc
         :jobflow-id nil      ;The jobflow-id, populated after the cluster is launched
         :request-ts nil      ;The timestamp of when the emr request was sent to AWS
         :validators []       ;A collection of validator functions
         :command-spec []     ;Command line options to parse for
         :profiles []         ;List of profiles to apply (left-to-right)
         :base {}             ;Base configuration
         :hooks []            ;Actions to be performed pre- or post- fire!
         :options {}          ;General options
         :option-defaults {}  ;The default values from the command-spec
         :remaining []        ;Remaining arguments from the command line after parsing out known options
         :raw-args []}))      ;Raw args from command-line

(defn context-get
  [k]
  (get @context k))

(defn context-set
  [k v]
  (swap! context #(assoc % k v)))

(defn context-update
  [keys f]
  (swap! context
         (fn [c] (update-in c keys f))))

(defmacro defcommand
  [sym]
  (let [name (-> sym name (#(subs % 0 (dec (count %)))))]
    `(defn ~sym [] (= (context-get :command) ~name))))

(defcommand local?)
(defcommand submit?)
(defcommand run?)
(defcommand dry-run?)
(defcommand start?)
(defcommand help?)
(defcommand formatted-help?)

(defn profile?
  "Test if the profile x is in use."
  [x]
  (some #{x} (context-get :profiles)))

(defn- process-args-if-needed
  "Trigger cli processing, if needed. Results are stored in @context."
  []
  (let [[defaults options remaining]
          (process-args (context-get :raw-args) (context-get :command-spec))]
    (context-set :remaining remaining)
    (context-set :option-defaults defaults)
    (context-set :options options))
  nil)

(defn- merged-opts
  []
  (merge (context-get :option-defaults) (context-get :options)))

(defn cli-opt
  "With no arguments, returns the options from the command line in a Map, where the
  keys are keywords, values are strings.  With a key argument, return the value for
  that key only."
  ([]
    (process-args-if-needed)
    (merged-opts))
  ([key]
    (process-args-if-needed)
    (get (merged-opts) key)))

(defn add-profiles
  "Add one or more profiles to the end of the current profile list."
  [profiles]
  (context-set :profiles
    (distinct
      (concat (context-get :profiles)
              (map #(if (keyword? %) % (keyword (subs % 1)))
                   (util/collectify profiles))))))

(defn- add-fns-to-coll
  "Filters from args all functions not preceded by nil/false."
  [context-key & args]
  (let [[new-fs _]
          (reduce (fn [[result last-cond] item]
                    (cond
                      (and last-cond (fn? item)) [(conj result item) true]
                      (and (not last-cond) (fn? item)) [result true]
                      :else [result item]))
                  [[] true]
                  args)]
    (context-update [context-key] #(concat % new-fs))))

(def add-validators (partial add-fns-to-coll :validators))

(def add-hooks (partial add-fns-to-coll :hooks))

(defn clear-validators
  []
  (context-set :validators []))

(defn clear-hooks
  []
  (context-set :hooks []))

(defn
  ^{:arglists '([[opt-name doc-string]+]
                [[opt-name doc-string default]+]
                [(:opt-name doc-string)+])}
  catch-args
  "args contains either vectors ([opt-name doc-string] or [opt-name doc-string default]),
  or alternating pairs of keyword/doc-string. The keyword is the option name, the string
  is the doc text. No default value is allowed when using the latter form.  Example:
    (catch-args :foo \"something about foo\"
                :bar \"bar help\"
                [:baz \"baz help\" 1])"
  [& args]
  (let [args-map (group-by vector? args)
        arg-vecs (get args-map true)
        arg-pairs (->> (get args-map false)
                    (partition 2))]
    (add-command-spec* context (concat arg-vecs arg-pairs))))

(defn- apply-profile
  "Override entries in the options with entries from the nested profile if one exists."
  [m profile]
  (let [profile-map (get m profile)]
    ; check for common errors
    (when (and profile-map (not (map? profile-map)))
      (quit :msg (format "The profile %s is not a Map; it's class is %s" profile (class profile-map))
            :exit-code 21))
    (when-let [arg-entries (seq (filter #(->> % key name (re-find #"^args\.")) profile-map))]
      (quit :msg (format (str "Profile %s contains %s. To override arg values with a profile, "
                              "specify the option name without the 'args.' prefix (e.g. {:foo 1} "
                              "instead of {:args.foo 1}).")
                         profile arg-entries)
            :exit-code 22))
    ; merge
    (if profile
      (dissoc (util/lemur-merge m profile-map) profile)
      m)))

(defn full-options
  "A merged map of all options. I.e. includes options from base, the command line
  and, optionally, the specified cluster.  Entries for a profile must have a map
  for a value, and the entries in that map override the entries with the same keys
  in the outer map. The profiles are applied in order (right-most having precedence).
  For example, if you have the following active profiles:
    [:ec2 :test]
  and options
    {:foo 100
     :bar 1
     :ec2 {:foo 200 :bar 2}
     :test {:bar 3}}
  Then full-options would return a map containing {:foo 200 :bar 3}."
  ([]
   (full-options nil))
  ([cluster]
   (full-options cluster nil))
  ([cluster step]
   (full-options nil cluster nil))
  ([base cluster step]
   (process-args-if-needed)
   ; The order that the options are accumulated is very important. Values from sources
   ; lower in the list override values from earlier in the list:
   ;   Start with (in order):
   ;     base
   ;     default-values-from-the-command-line
   ;     cluster
   ;     step
   ;   Now apply profiles
   ;     we do this now, so that a profile in a cluster (or base) can override a value in a step
   ;   Next, the actual command line, and the :command key
   ;     explicit command-line args trump all
   ;     the single value for :command is applied (i.e. dry-run, start, etc)
   ;   Finally
   ;     remove all entries where the value is nil
   (let [cluster-step-opts
           (util/lemur-merge
             (or base (context-get :base))
             (context-get :option-defaults)
             cluster
             step)
         opts-after-profiles
           (reduce apply-profile cluster-step-opts (context-get :profiles))
         opts-after-command-line
           (util/lemur-merge
             opts-after-profiles
             (context-get :options)
             (select-keys @context [:command :jobdef-path :remaining]))]
     (->> opts-after-command-line
       (filter #(-> % val nil? not))
       (into {})))))

(defn evaluating-step
  ([cluster step]
   (evaluating-step nil cluster step))
  ([base cluster step]
   (lemur.evaluating-map/evaluating-map (full-options base cluster (if-not (instance? StepConfig step) step)))))

; Using a distinct name for this function (as opposed to use)
; is good for calling out the intention when used in a job-def.
(util/defalias use-base clojure.core/use
  "Specify namespaces that should be required, thereby applying any updates to
   the default settings.
   The namespace would presumably make use of (update-base), (add-profile),
   etc.; to impact the default behaviors and options for the job.
   Multiple name-spaces can be specified, and they are applied in order (left to
   right), with the rightmost taking precedence.")

(defn update-base
  [m]
  "Update the default behaviors and options in the context base.  For example, pass
  in a map like {:num-instances 10} to change the default number of instances for
  a new EMR cluster to 10.  Each value in the map can be a literal or a function
  (see the note on MAP VALUES in sample-jobdef.clj)"
  (context-update [:base] #(util/lemur-merge % m)))

(defn- enable-debugging-step?
  [step]
  (and (instance? StepConfig step)
    (-> step .getName (= "Enable Debugging"))))

(defn- step-jar
  [estep]
  (cond
    (:step-jar estep)
      (:step-jar estep)
    (and (:runtime-jar estep) (:copy-runtime-jar? estep))
      (str (:jar-uri estep) "/" (ccio/basename (:runtime-jar estep)))
    (:runtime-jar estep)
      (:runtime-jar estep)
    :else
      (str (:jar-uri estep) "/" (ccio/basename (:jar-src-path estep)))))

(defn- step-args
  [estep]
  ; error on extra args, not in defstep
  (let [args-in-step-without-order
          (-> estep
            keys
            set
            (->> (filter #(re-find #"^args\." (name %))))
            seq
            set
            (clojure.set/difference (set (seq (:args-order estep)))))]
    (when (pos? (count args-in-step-without-order))
      (quit
        :msg (format (str "These args %s have values, but are not specified explicitly in "
                          "defstep (%s).  Args to your hadoop job step need to be explicitly "
                          "specified in defstep, so lemur will know what order to put them "
                          "in.  Remember, you can specify ':args.foo nil' to explicitly "
                          "state order for an arg that may not always be present.")
                     (seq args-in-step-without-order)
                     (:step-name estep))
        :exit-code 20)))
  ; produce the args
  (->>
    (for [arg-key (:args-order estep)]
      (condp = arg-key
        :args.passthrough
          (if (:args.passthrough estep) (:remaining estep))
        :args.data-uri
          (if (:args.data-uri estep) (:data-uri estep))
        :args.positional
          (:args.positional estep)
        (let [val (get estep arg-key)
              opt-name (str "--" (second (re-find #"^args\.(.+)$" (name arg-key))))]
          (cond
            (nil? val) nil
            (true? val) opt-name
            (false? val) nil
            (= "nil" val) nil
            (and (string? val) (empty? val)) nil
            :default [opt-name val]))))
    flatten
    (filter (complement nil?))
    (map str)))

(defn- mk-steps
  "Takes cluster and a collection of objects created by defstep, and returns
  a seq of StepConfig objects."
  [cluster coll]
  (map
    (fn [step]
      (if (instance? StepConfig step)
        step
        (let [estep
                (evaluating-step cluster step)
              args
                (step-args estep)
              path-to-jar
                (step-jar estep)]
          (emr/step-config
            (:step-name estep)
            (:keep-alive? estep)
            path-to-jar
            (:main-class estep)
            args
            :action-on-failure (:action-on-failure estep)
            :properties (:properties estep)))))
    coll))

(defn- save-metajob
  [cluster steps metajob]
  (let [eopts (lemur.evaluating-map/evaluating-map (full-options cluster) steps)]
    (when (:metajob-file eopts)
      ; TODO create a temp file here, and use ccio/cp
      (if (s3/s3path? (:base-uri eopts))
        (let [[bkt key] (s3/parse-s3path (:base-uri eopts))]
          (s3/put-object-string
            bkt
            (str (s3/slash$ key) "METAJOB.yml")
            (s/join "---\n" metajob)))
        (let [base-dir (io/file (:base-uri eopts))]
          (.mkdirs base-dir)
          (spit (io/file base-dir "METAJOB.yml")
                (s/join "---\n" metajob)))))))

(defmulti launch
  (fn [command & args]
    (condp = command
      "run" :run-or-start
      "start" :run-or-start
      (keyword command))))

(defmethod launch :local
  [command _ cluster steps jobflow-options uploads metajob]
  (let [steps (filter (complement enable-debugging-step?) steps)]
    (save-metajob cluster steps metajob)
    (doseq [step steps]
      (let [estep
              (evaluating-step cluster step)
            hadoop-bin
              (s/trim (:out (sh/sh "which" "hadoop" :err :pass)))
            hadoop-version
              (when-not (empty? hadoop-bin)
                (re-find #"^Hadoop [^\n]+" (:out (sh/sh hadoop-bin "version" :err :pass))))
            args
              (step-args estep)]

        ; copy files locally (think about making these symlinks instead)
        (util/upload (:show-progress? estep) (:jars uploads))
        (util/upload (:show-progress? estep) (:jobdef-cluster uploads) (:dest-working-dir uploads))
        (util/upload (:show-progress? estep) (mapcat val (:jobdef-step uploads)) (:dest-working-dir uploads))

        ; verify hadoop location and version

        (when (empty? hadoop-bin)
          (quit :exit-code 12 :msg (str "Could not find the hadoop binary. Install Hadoop 0.20.* "
                                        "locally and make sure 'hadoop' is in your path.")))

        (when (not (re-find #"Hadoop 0\.20\." hadoop-version))
          (log/warn (str "If you are testing for EMR, it is best to run with a 0.20.x "
                         "version of Hadoop. But you have: " hadoop-version)))

        ; TODO Allow pass-through of hadoop options like "--conf <dir>"
        (log/warn "Using your locally installed hadoop binaries and configuration.")

        (let [hadoop-cmd [hadoop-bin "jar" (:jar-src-path estep) (:main-class estep)]
              cmd (concat hadoop-cmd args)]

          ; Print the full hadoop command line.  In theory, users should be able to
          ; copy+paste this output in order to run the hadoop job locally and
          ; bypass lemur.
          (-> (:hadoop-env estep)
            (->> (map #(format "%s=\"%s\"" (name (key %)) (val %))))
            (concat hadoop-cmd)
            (concat (map #(if (re-find #"^--" %) % (str \" % \")) args))
            (->> (s/join " "))
            println)

          ; sh/sh will block, the hadoop process will output to stdout/stderr
          (let [result (apply sh/sh (concat cmd [:out :pass :env (sh/merge-env (:hadoop-env estep))]))]
            (if (zero? (:exit result))
              0
              (quit :msg "hadoop process failed" :exit-code (:exit result)))))))))

(defmethod launch :run-or-start
  [command eopts cluster steps jobflow-options uploads metajob]
  ; upload files
  (if (run?)
    (util/upload (:show-progress? eopts) (mapcat val (:jobdef-step uploads)) (:dest-working-dir uploads)))
  (util/upload (:show-progress? eopts) (:bootstrap-actions uploads))
  (util/upload (:show-progress? eopts) (:jars uploads))
  (util/upload (:show-progress? eopts) (:jobdef-cluster uploads) (:dest-working-dir uploads))
  ; Write details to metajob file
  (save-metajob cluster steps metajob)
  ; launch
  (let [request-ts (System/currentTimeMillis)
        steps (if (run?)
                (mk-steps cluster steps)
                (filter enable-debugging-step? steps))
        jobflow-id (emr/start-job-flow
                     (:emr-name eopts)
                     steps
                     jobflow-options)]
    (println "JobFlow id:" jobflow-id)
    (context-set :jobflow-id jobflow-id)
    (context-set :request-ts request-ts)))

(defmethod launch :submit
  [command eopts cluster steps jobflow-options uploads metajob]
  ; upload files
  (util/upload (:show-progress? eopts) (mapcat val (:jobdef-step uploads)) (:dest-working-dir uploads))
  (util/upload (:show-progress? eopts) (:jars uploads))
  ; Write details to metajob file
  (save-metajob cluster steps metajob)
  ; launch
  (let [request-ts (System/currentTimeMillis)
        steps (mk-steps cluster (filter (complement enable-debugging-step?) steps))
        jobflow-id (:jobflow eopts)]
    (emr/add-steps jobflow-id steps)
    (println "JobFlow id:" jobflow-id)
    (context-set :jobflow-id jobflow-id)
    (context-set :request-ts request-ts)))

(defmethod launch :dry-run
  [command eopts cluster steps jobflow-options uploads metajob]
  (println "dry-run, not launching."))

(defn- steps-details
  [cluster steps]
  (let [steps-to-print (map (fn [step]
                              (if (map? step)
                                (let [estep (evaluating-step cluster step)]
                                  (assoc (select-keys estep [:main-class :step-name])
                                         :args (step-args estep)
                                         :step-jar (step-jar estep)))
                                step))
                            steps)]
    steps-to-print))

(defn- uploads-details
  [{:keys [dest-working-dir jars bootstrap-actions jobdef-cluster jobdef-step]}]
  (let [upload-to-str
          (fn [[src dest]]
            (str src " -> " (util/mk-absolute-path dest-working-dir dest)))]
    {"Jars"
       (map upload-to-str jars)
     "Bootstrap Actions"
       (when-not (local?)
         (map upload-to-str bootstrap-actions))
     "Cluster Files"
       (map upload-to-str jobdef-cluster)
     "Step Files"
       (map (fn [[step-name step-upload]] [step-name (map upload-to-str step-upload)]) jobdef-step)}))

(defn- parse-upload-property
  "Construct upload-directives suitable for util/upload, from the more human-friendly
  value of (:upload eopts).  A keyword argument other than :to is looked up in eopts.

  EXAMPLE
  this structure
     [\"file\", \"bar/file\" :to \"COUNTIES\", \"baz\", \"input\" :to \"cropy/\"]
  would result in upload-directives:
     [[\"file\"] [\"bar/file\" \"COUNTIES\"] [\"baz\"] [\"input\" \"cropy/\"]]"
  [eopts]
  (get
    (reduce
      (fn [{:keys [acc to]} x]
        (let [xval (if (and (keyword? x) (not= :to x)) (get eopts x) x)]
          (cond
            (or (and to (string? xval) (empty? acc))
                (and to (= xval :to))
                (and to (coll? xval)))
              (quit :msg (format "Could not parse upload property (%s) in %s" x (:upload eopts)) :exit-code 1)
            (and to (string? xval))
              {:acc (conj (vec (butlast acc)) [(first (last acc)) xval]) :to false}
            (string? xval)
              {:acc (conj acc [xval]) :to false}
            (= xval :to)
              {:acc acc :to true}
            (coll? xval)
              {:acc (conj acc (filter #(not= :to %) xval)) :to false}
            :default
              (quit :msg (format "Could not parse upload property (%s) in %s" x (:upload eopts)) :exit-code 1))))
      {:acc [] :to false}
      (:upload eopts))
    :acc))

(defn- fire*
  [command cluster steps]
  (let [evaluating-opts (lemur.evaluating-map/evaluating-map (full-options cluster) steps)
        ;When (dry-run?), validate won't exit immediately, so save the results to output at the end
        validation-result (validate
                            (context-get :validators)
                            evaluating-opts
                            (context-get :command-spec)
                            (when-not (dry-run?) 3))]

    (log/debug "FULL EVALUATING OPTIONS" evaluating-opts)

    ;;; config

    (when-let [endpoint (:endpoint evaluating-opts)]
      (.setEndpoint emr/*emr* endpoint))

    (let [[spot-task-type spot-task-bid spot-task-num]
            (emr/parse-spot-task-group (:spot-task-group evaluating-opts))
          ba-vector (doall (mk-bootstrap-actions evaluating-opts))
          jobflow-options
            {:bootstrap-actions (map second ba-vector)
             :log-uri (:log-uri evaluating-opts)
             :availability-zone (:availability-zone evaluating-opts)
             :master-type (:master-instance-type evaluating-opts)
             :slave-type (:slave-instance-type evaluating-opts)
             :num-instances (util/as-int (:num-instances evaluating-opts))
             :spot-task-type spot-task-type
             :spot-task-bid spot-task-bid
             :spot-task-num spot-task-num
             :keep-alive (or (:keep-alive? evaluating-opts) (start?) (submit?))
             :keypair (:keypair evaluating-opts)
             :service-role (:service-role evaluating-opts)
             :job-flow-role (:job-flow-role evaluating-opts)
             :ami-version (:ami-version evaluating-opts)
             :hadoop-version (:hadoop-version evaluating-opts)
             :supported-products (:supported-products evaluating-opts)
             :visible-to-all-users (true? (:visible-to-all-users evaluating-opts))
             :subnet-id (:subnet-id evaluating-opts)}
          steps
            (if (:enable-debugging? evaluating-opts)
              (cons (emr/debug-step-config) steps)
              steps)
          uploads  ; Constructs upload-directives suitable for (util/upload)
            {:dest-working-dir
               (:data-uri evaluating-opts)
             :jars
               (cond
                 (and (:runtime-jar evaluating-opts) (:copy-runtime-jar? evaluating-opts))
                   [[(:runtime-jar evaluating-opts) (step-jar evaluating-opts)]]
                 (not (:runtime-jar evaluating-opts))
                   [[(:jar-src-path evaluating-opts) (step-jar evaluating-opts)]]
                 (:runtime-jar evaluating-opts)
                   [])
             :bootstrap-actions
               (let [src (:scripts-src-path evaluating-opts)]
                 (if (and src (not (submit?)))
                   [[(s3/slash$ src)
                     (s3/s3path (:bucket evaluating-opts) (:std-scripts-prefix evaluating-opts))]]
                   []))
             :jobdef-cluster
               (if (submit?) [] (parse-upload-property evaluating-opts))
             :jobdef-step
               (->> steps
                (filter map?)
                ; filter from the original defstep construct, rather than the estep, so that
                ; we don't get :upload entries from the base or cluster
                (filter :upload)
                ; Make sure we don't pick up any upload directives from the base or cluster
                (map (partial evaluating-step (dissoc (context-get :base) :upload) (dissoc cluster :upload)))
                (map (juxt :step-name parse-upload-property))
                (into {}))}
          display-in-metajob
            (util/collectify (:display-in-metajob evaluating-opts))
          metajob
            (filter identity (flatten
              [(util/mk-yaml "Profiles" (map str (context-get :profiles))
                 (doto (DumperOptions.) (.setDefaultScalarStyle DumperOptions$ScalarStyle/PLAIN)))
               (util/mk-yaml "Command Line Options"
                 (->> (context-get :command-spec)
                   (map first)
                   ; don't include the default options-- they are represented in Environment or Joblow Options below
                   (#(clojure.set/difference (set %) (set (map first init-command-spec))))
                   (select-keys evaluating-opts))
                 (doto (DumperOptions.) (.setDefaultFlowStyle DumperOptions$FlowStyle/BLOCK)))
               (util/mk-yaml "Environment"
                 (select-keys
                   evaluating-opts
                   (concat [:command :app :comment :username :run-id :jar-src-path :runtime-jar :base-uri]
                           display-in-metajob))
                 (doto (DumperOptions.) (.setDefaultFlowStyle DumperOptions$FlowStyle/BLOCK)))
               (if-not (or (local?) (submit?))
                 (vector
                   (util/mk-yaml "Jobflow Options"
                     (merge (select-keys evaluating-opts [:emr-name]) (dissoc jobflow-options :bootstrap-actions))
                     (doto (DumperOptions.) (.setDefaultFlowStyle DumperOptions$FlowStyle/BLOCK)))
                   (util/mk-yaml "Hadoop Config" (hadoop-config-details evaluating-opts)
                     (doto (DumperOptions.) (.setDefaultFlowStyle DumperOptions$FlowStyle/BLOCK)))
                   (util/mk-yaml "Bootstrap Actions" (bootstrap-actions-details ba-vector)
                     (doto (DumperOptions.) (.setDefaultScalarStyle DumperOptions$ScalarStyle/PLAIN)))))
               (util/mk-yaml "Hooks" (map (comp (memfn getName) class) (context-get :hooks))
                 (doto (DumperOptions.) (.setDefaultScalarStyle DumperOptions$ScalarStyle/PLAIN)))
               (util/mk-yaml "Uploads" (uploads-details uploads)
                 (doto (DumperOptions.) (.setDefaultFlowStyle DumperOptions$FlowStyle/BLOCK)))
               (util/mk-yaml "Steps" (steps-details cluster steps)
                 (doto (DumperOptions.) (.setDefaultScalarStyle DumperOptions$ScalarStyle/PLAIN)))]))]

      ;;; informational

      ; Write details to stdout
      (println "")
      (println (s/join metajob))
      (println "")

      ;;; trigger hooks and launch

      ; hooks w/ arity 1 are called pre-launch, those w/ arity 2 are called post-launch
      (let [eopts-with-mj (lemur.evaluating-map/evaluating-map (assoc (full-options cluster) :lemur-metajob metajob) steps)
            ;pre-hooks
            hook-precall-results
              (doall (for [f (context-get :hooks)]
                (when (util/has-arity? f 1) (f eopts-with-mj))))]
        ; launch the job
        (launch command evaluating-opts cluster steps jobflow-options uploads metajob)
        ; post-hooks
        (doall
          (for [[f state] (reverse (map vector (context-get :hooks) hook-precall-results))]
            (when (util/has-arity? f 2)
              (f eopts-with-mj state)))))

      (when validation-result
        (println validation-result))

      ;;; return metajob
      metajob)))

(defn- steps-for-active-profiles
  [args]
  (let [[always-on profile-steps]
          (split-with (complement keyword?) args)
        active-profiles
          (context-get :profiles)]
    (->> profile-steps
           (partition 2)
           (filter (fn [[profile _]] (some #{profile} active-profiles)))
           (map second)
           (mapcat util/collectify)
           (concat always-on)
           flatten)))

(defn fire!
  "Based on the command, take action using the given cluster and steps.

  [(cluster|cluster-fn) (steps*|step-fn)]

  The first arg is a cluster (created by defcluster) or a 'fn of eopts' that returns a cluster.

  Scond arg is one or more steps, either as a collection or a variable length
  argument list.  Alternatively, you can provide a single 'fn of eopts' which returns
  a step or collection of steps. Each step is a defstep or a StepConfig.

  Examples:

    (fire! my-cluster a-step b-step)
    (fire! my-cluster step-selector-fn)

  Returns the metajob, which is a YAML string with all the details for the launch"
  [cluster-arg & profile-steps]
  (process-args-if-needed)
  (when (local?)
    (add-profiles :local))
  (cond
    (help?)
      (quit :cmdspec (context-get :command-spec) :exit-code 0)
    (formatted-help?)
      (do
        (println "---")
        (println (formatted-help (context-get :command-spec)))
        (quit :exit-code 0))
    :default
      (let [cluster (if (fn? cluster-arg)
                      (cluster-arg (lemur.evaluating-map/evaluating-map (full-options)))
                      cluster-arg)
            steps
              (if (fn? (first profile-steps))
                ((first profile-steps) (lemur.evaluating-map/evaluating-map (full-options cluster)))
                ; profile steps usage is deprecated -- it was overly complicated, use a fn instead
                (steps-for-active-profiles profile-steps))]
        (fire* (context-get :command) cluster steps))))

(defn- execute-jobdef
  [file]
  (when-not file
    (quit :msg "No jobdef file was supplied" :exit-code 1
          :cmdspec (context-get :command-spec)))
  (let [job-ns (gensym (-> file ccio/basename (s/replace #"\.clj$" "")))]
    (log/debug (str "Jobdef namespace " job-ns))
    (binding [*ns* (create-ns job-ns)]
      (refer-clojure)
      (use 'lemur.core)
      (use 'lemur.common)
      (use 'lemur.command-line)
      ; set some common namespace aliases for convenience
      (require '[clojure.java.io                :as io])
      (require '[clojure.string                 :as s])
      (require '[clojure.tools.logging          :as log])
      (require '[com.climate.shell              :as sh])
      (require '[lemur.util                     :as util])
      (require '[com.climate.services.aws.emr   :as emr])
      (require '[com.climate.services.aws.s3    :as s3])
      (require '[com.climate.services.aws.ec2   :as ec2])
      ; Include the standard base file
      (use-base 'lemur.base)
      ; load the job file here... it is up to the job file to call fire! with
      ; the appropriate cluster and step(s)
      (log/info (str "Loading jobdef " file))
      (load-file file))))

(defn wait
  "After the job has been launched, wait (block) for the cluster to reach the
  specfied stage:
    :provisioned
    :accepted
    :ready
  This function is a no-op for dry-run or local."
  [stage]
    (when (#{"run" "start"} (context-get :command))
      (let [jobflow-id (context-get :jobflow-id)]
        (condp = stage
          :provisioned (emr/wait-until-provisioned jobflow-id)
          :accepted (emr/wait-until-accepted jobflow-id)
          :ready (emr/wait-until-ready jobflow-id)
          (throw (IllegalArgumentException. (str "Unknown stage " stage)))))))

(defn wait-on-step
  "After the job has been launched, wait (block) for the specified step to
  complete.  You must also specify a timeout in seconds, for the maximum
  amount of time to wait. Returns a map with possible keys
    :success - a boolean for whether the job completed sucessfully
    :timeout - a boolean for whether the timeout was reached
    :step-status-detail - a string
    :job-status-detail - a string
  This function is a no-op for dry-run, start or local."
  [step timeout-seconds]
    (when (#{"run"} (context-get :command))
      (emr/wait-on-step
        (context-get :jobflow-id)
        (step :step-name)
        timeout-seconds)))

(defmacro defcluster
  [name-sym & bindings]
  (let [cluster-name (s/replace (name name-sym) #"-cluster$" "")]
   `(def ~name-sym
      (assoc
        (hash-map ~@bindings)
        :jobdef-file (util/shortname *file*)
        :cluster-name ~cluster-name))))

(defn- matching-eoval-expr?
  [k v]
  (let [v-meta (meta v)]
    (and (:lemur.evaluating-map/eoval v-meta) (= (:lemur.evaluating-map/key v-meta) k))))

(defn- get-opt-key
  "Get the name of the key. The name is suffixed with ? in the boolean case, i.e. either:
   a) value is a Boolean (true or false)
   b) value is an eoval expression of the same name as k with ? suffix."
  [k v]
  (let [arg-suffix (-> k name (subs 5))
        arg-suffix-bool (str arg-suffix "?")]
    (keyword (if (or (instance? Boolean v)
                     (matching-eoval-expr? (keyword arg-suffix-bool) v))
               arg-suffix-bool
               arg-suffix))))

(defmacro defstep
  "Creates a map, which represents a single EMR step"
  [name-sym & bindings]
  (let [step-name
          (s/replace (name name-sym) #"-step$" "")
        args-order
          (->> bindings (take-nth 2) (filter #(re-find #"^args\." (name %))))
        step-map
          (doall
            (reduce
              (fn [sm arg-key]
                (let [orig-val (eval (get sm arg-key))
                      opt-key (get-opt-key arg-key orig-val)]
                  ; test if this arg is explicit (i.e. appears in catch-args)
                  (if (->> (context-get :command-spec) (map first) (filter #(= opt-key %)) seq)
                    sm
                    ; if not explicit, create implicit command line arg
                    (let [opt-name (name opt-key)]
                      (catch-args [opt-key
                                   opt-name
                                   (cond
                                     (= orig-val (str "${" opt-name "}"))  nil
                                     (matching-eoval-expr? opt-key orig-val) nil
                                     :else orig-val)])
                      ; adjust the values in the step map, to be fn's of their opt-key's
                      (assoc sm arg-key (fn [eopts] (get eopts opt-key)))))))
              (apply hash-map bindings)
              (remove #(some #{%} [:args.data-uri :args.positional :args.passthrough]) args-order)))]
    ; warn if args.* contains entries that end with '?'
    (when-let [args-ending-in-? (seq (filter #(re-find #"\?$" (name %)) args-order))]
      (log/warnf
          (str "\nWARNING\n In defstep %s, these args end in ?: %s. You probably don't want "
               "this. To indicate a boolean arg, just specify a boolean value (true/false). "
               "The text after 'args.' is used to form the commandline options sent to "
               "the hadoop main class. For example, :args.foo? would create a commandline "
               "option --foo?, presumably you just want --foo or nothing. However, if "
               "you define ':args.foo false' in your defstep, it will correctly be "
               "associated with implicit catch-arg :foo? since it is a boolean.")
          step-name args-ending-in-?))
    ; def the step-map
    `(def ~name-sym
      (merge
        {:step-name ~step-name
         :args-order '~args-order}
        ~step-map))))

(defmacro lfn
  "Creates a memoized function of the given keys (from eopts). Primarily, these functions are
  intended to be used as values in defcluster/defstep/etc.  As such, the functions may be
  called multiple times through evaluating-map. You would generally want the fn's to be
  idempotent and not to have side-effects. But if there are side-effects, you only want
  them to happen once.
  For example, I have one usage that creates a temp file and returns the path to the temp file.
  If it wasn't memoized, it would wind up creating multiple temp files, although only one would
  get used."
  [key-syms & body]
  `(let [f# (fn [~@key-syms] ~@body)
         memof# (memoize f#)]
    (fn [{:keys [~@key-syms]}] (memof# ~@key-syms))))

(defmacro with-eopts
  "Example
    (with-eopts [cluster step?] [eopts]
      (println (:cluster-name eopts)))
  The cluster and (optionally) the step are used to create an evaluating map. The body is
  evaluated with the evaluating map bound to the symbol eopts.  In most cases, prefer
  the add-hooks functionality over this method.  This method may become deprecated
  in the future."
  [[cluster step] [eopts-sym] & body]
  `(let [~eopts-sym (lemur.evaluating-map/evaluating-map (full-options ~cluster ~step))]
     ~@body))

(defn- display-types
  []
  (let [flds ["Family" :family "Instance" :type "CPUs" :cpu "Arch" :arch "Mem (gb)" :mem
              "IO speed" :io "Demand $" :us-east-demand "Reserve $" :us-east-reserve]
        extract-flds
          (apply juxt (take-nth 2 (rest flds)))
        details
          (->> (vals ec2/ec2-instance-details)
            (sort-by (juxt :family :type))
            (map extract-flds))]
    (cl-format true "~{~12<~a~>~}~%~
                     ~{~{~12<~a~>~}~%~}"
                    (take-nth 2 flds)
                    details)
    (flush))
  (quit))

(defn- spot-price-history
  []
  (let [flds ["Instance" (memfn getInstanceType) "Zone" (memfn getAvailabilityZone)
              "Timestamp" (memfn getTimestamp) "Spot $" (memfn getSpotPrice)]
        extract-flds
          (apply juxt (take-nth 2 (rest flds)))
        details
          (->> (ec2/spot-price-history 24)
            (map extract-flds)
            sort)]
    (cl-format true "~{~12<~a~>~}~%~
                     ~{~{~12<~a~>~}~%~}"
                    (take-nth 2 flds)
                    details)
    (flush))
  (quit))

(defmacro when-local-test
  "Execute the body if the :test profile is active and lemur was run with the local command."
  [& body]
  `(when (and (profile? :test) (local?))
     true ;default if body is empty
     ~@body))

(defn -main
  "Run lemur help."
  [& [command & args]]
  (add-command-spec* context init-command-spec)
  (let [[profiles remaining] (split-with #(.startsWith % ":") args)
        aws-creds (awscommon/aws-credential-discovery)
        jobdef-path (first remaining)]
    (add-profiles profiles)
    (context-set :jobdef-path jobdef-path)
    (context-set :raw-args (rest remaining))
    (context-set :command command)
    (binding [s3/*s3* (s3/s3 aws-creds)
              emr/*emr* (emr/emr aws-creds)
              ec2/*ec2* (ec2/ec2 aws-creds)]
      (case command
        ("run" "start" "dry-run" "local" "submit")
          (execute-jobdef jobdef-path)
        "display-types"
          (display-types)
        "spot-price-history"
          (spot-price-history)
        "version"
          (quit :msg (str "Lemur " (System/getenv "LEMUR_VERSION")))
        "formatted-help"
          (execute-jobdef jobdef-path)
        "help"
          (if jobdef-path
            (execute-jobdef jobdef-path)
            (quit :msg (slurp (io/resource "help.txt"))))
        (quit :msg (if command (str "Unrecognized lemur command: " command))
              :cmdspec (context-get :command-spec) :exit-code 1))))
  (quit))
