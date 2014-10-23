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

(ns lemur.command-line
  (:use
    [clojure.stacktrace :only [print-stack-trace]]
    [clojure.pprint :only [cl-format]])
  (:require
    [lemur.util :as util]
    [clojure.string :as s]
    [clojure.tools.logging :as log])
  (:import
    [org.yaml.snakeyaml DumperOptions
                        DumperOptions$FlowStyle
                        DumperOptions$ScalarStyle]))

(def usage-text "
  lemur is a tool to launch hadoop jobs locally or on EMR based on a configuration
  file, referred to as a jobdef. The general command line format is:

  lemur <command> [profiles*] <jobdef-file> [options] [remaining]

  lemur help                      - Display an overview and help about the jobdef file
  lemur version                   - Display the verision number
  lemur run jobdef.clj            - Run a job on EMR
  lemur dry-run jobdef.clj        - Dry-run, i.e. just print out what would be done
  lemur start jobdef.clj          - Start an EMR cluster, but don't run the steps (jobs)
  lemur local jobdef.clj          - Run the job using local hadoop (e.g. standalone mode)
  lemur formatted-help jobdef.clj - Returns yaml about the options for the given jobdef
                                    This is intended for machine consumption (to drive
                                    a generic UI, for example)

  lemur display-types      - Display the instance-types with basic stats and exit
  lemur spot-price-history - Display the spot price history for the last day and exit

  You can optionally list one or more profiles after the command.  Each one must start
  with a colon.  See 'lemur help' for an explanation of profiles.

  Examples:
  lemur run :test src/main/clj/lemur/sample-jobdef.clj --foo 1 --bar blah
  lemur start src/main/clj/lemur/sample-jobdef.clj")

(defmacro pddd
  "push-down-default-doc. If you have a var with :default-doc metadata, return the
  underlying object with the :default-doc in it's metadata."
  [sym]
  `(let [default-doc# (:default-doc (meta (var ~sym)))]
     (if default-doc#
       (with-meta ~sym (assoc (meta ~sym) :default-doc default-doc#))
       ~sym)))

(defn- default-doc
  [f]
  (-> f meta :default-doc))

(defn- print-usage [cmd-spec]
  (println "USAGE\n" usage-text "\n\nOPTIONS\n")
  (let [cmd-for-display
          (fn [[key docstr default]]
            (if (re-find #"\?$" (name key))
              [(s/replace (name key) #"\?$" "") docstr (true? default)]
              [(name key) docstr (or (default-doc default) default)]))
        cmds
          (map cmd-for-display cmd-spec)
        longest
          (apply max (map #(-> % first name count inc) cmd-spec))]
    (cl-format true (str "~:{~2T--~" longest "a ~a (DEFAULT: ~a)~%~}") cmds))
  (println ""))

(defn formatted-help
  [command-spec]
  (util/mk-yaml
      "OPTIONS"
      (map
        (fn [[key docstr default]]
          (if (re-find #"\?$" (name key))
              [key docstr (true? default)]
              [key docstr (or (default-doc default) default)]))
        command-spec)
      (doto (DumperOptions.) (.setDefaultScalarStyle DumperOptions$ScalarStyle/PLAIN))))

(defn- quit* [{:keys [msg cmdspec exception exit-code] :or {exit-code 0}}]
  (if cmdspec (print-usage cmdspec))
  (if (and msg (= 0 exit-code)) (println msg "\n"))
  (if (and msg (not= 0 exit-code)) (println (format "ERROR%n%n%s%n" msg)))
  (if exception (print-stack-trace exception))
  (System/exit exit-code))

(defn quit [& {:keys [msg cmdspec exception exit-code] :as args :or {exit-code 0}}]
  (quit* args))

(defn quit-by-error [ex]
  (quit* (:data (ex-data ex))))

(defn error [& {:keys [msg cmdspec exception exit-code] :as data :or {exit-code 0}}]
  (throw (ex-info msg {:data data})))

(defn- defaults-from-cmd-spec
  "cmd-spec is a coll of tuples. Extract the default values (i.e. the optional
  third arg of each tuple) and return as a map."
  [cmd-spec]
  (merge
    ; find ? vars (which have an implied default of false)
    (->> cmd-spec
      (filter #(-> % first name last (= \?)))
      (map (fn [[k _]] [k false]))
      (into {}))
    ; find explicit defaults
    (->> cmd-spec
      (filter #(= 3 (count %)))
      (filter #(nth % 2))
      (mapcat (fn [[name _ default]] [name default]))
      (apply hash-map))))

(defn- parse-args
  "Custom command line parsing.
  cmd-spec is a collection of vectors, each vector is an option of the form:
  [keyword \"doc string\" default-value].  This is similar to
  clojure.contrib.command-line/with-command-line except the option name is a
  keyword or string, not a symbol; and the final argument for remaining args
  should not be included."
  ([cmd-spec args]
    ; Execution starts here
    (let [[options remaining look-for]
            (reduce (partial parse-args cmd-spec) [{} [] nil] args)]
      [(defaults-from-cmd-spec cmd-spec) options remaining look-for]))
  ([cmd-spec [options remaining look-for] orig-arg]
    ; Continue by processing one arg at a time
    (let [as-keyword #(->> % (re-find #"^--(.*)") second keyword)
          option? (re-find #"^--" orig-arg)
          bang-option? (re-find #"^--no-(.*)$" orig-arg)
          arg (if bang-option? (str "--" (second bang-option?)) orig-arg)
          find (fn [x] (seq (filter #(->> % first name (str "--") (= x)) cmd-spec)))
          in-cmd-spec? (find arg)
          boolean-in-cmd-spec? (find (str arg "?"))]
      (cond
        ;Looking for a value for a previous option (the value can start
        ;with -- and still be considered a value)
        look-for
          (do
            ; warn on nil or null as a string
            (when (or (= arg "null") (= arg "nil"))
              (log/warn (format
                          "Option --%s was given value %s. This is a string, if you actually want to supply 'no value', use --no-%s"
                          (name look-for) arg (name look-for))))
            [(assoc options look-for arg) remaining nil])
        ;in cmd-spec, with bang, not boolean
        (and option? bang-option? in-cmd-spec?)
          [(assoc options (as-keyword arg) nil) remaining nil]
        ;in cmd-spec, with bang, boolean
        (and option? bang-option? boolean-in-cmd-spec?)
          [(assoc options (as-keyword (str arg "?")) false) remaining nil]
        ;in cmd-spec, needs value
        (and option? in-cmd-spec?)
          [options remaining (as-keyword arg)]
        ;in cmd-spec, boolean
        (and option? boolean-in-cmd-spec?)
          [(assoc options (as-keyword (str arg "?")) true) remaining nil]
        ;not an option or not ours
        :else
          [options (conj remaining orig-arg) nil]))))

(defn extract-cl-args
  "An alternate command line parser that takes an abbreviated cmd-spec. This is useful
  for writing a validator function.  mini-cmd-spec is a coll of the option names as
  keywords (e.g. [:foo :bar?]). You only need to specify the options you are
  interested in validating.
  Returns "
  [mini-cmd-spec args]
  (let [cmd-spec (map util/collectify mini-cmd-spec)
        [defaults options remaining _] (parse-args cmd-spec args)]
    [(merge defaults options) remaining]))

(defn validate
  "Run the validators on eopts. If failure, quit with the given exit-code and
  use the cmd-spec to print help. Each validator is a function that returns a
  String, vector of Strings or false on failure. Success if nil or true or '().
  If exit-code is nil, then don't exit, just print a warning)."
  [validators eopts cmd-spec exit-code]
  (let [errors
          (->> (for [v validators
                     :let [result (v eopts)]]
                 (if (false? result)
                   (str "Failed: " v) ;TODO rather than just v, get the source-code of v from meta
                   result))
            flatten
            (keep identity)
            (filter (complement true?)))
        error-msg
          (cl-format nil "~%VALIDATION FAILED~%~%~{~2T~a~%~}" errors)
          ]
    (when (seq errors)
      (if exit-code
        (quit :msg error-msg :cmdspec cmd-spec :exit-code exit-code)
        (log/warn error-msg))
      error-msg)))

(defn process-args
  "Parse the args (presumably from the command-line) and return the
  options and remaining positional args."
  [args command-spec]
  (let [[defaults options remaining look-for]
          (try
            (parse-args command-spec args)
            (catch Exception e
              (quit :cmdspec command-spec :exception e :exit-code 1)))]
    (if look-for
      (quit
        :msg (format "Command line option --%s expected a value, but did not get one." (name look-for))
        :cmdspec command-spec
        :exit-code 2))
    [defaults options remaining]))

(defn add-command-spec*
  "Specify additional command line options to accept.
  Each entry in the coll is [option-name doc-string default-value?]
  where option-name is a keyword.  If the option is a boolean flag, it's name
  should end with '?', and then no value will be expected on the command line.
  This function can be called multiple times (e.g. from a base and from a job-def),
  in which case each call appends the new options.  If two or more calls try to
  specify options with the same name, this is an error and an IllegalStateException
  will be thrown."
  [context coll]
  (swap!
    context
    (fn [c]
      (update-in c [:command-spec]
        (fn [old]
          (let [result (concat old coll)
                ; check for duplicate names
                names (->> result (map first) (map name))
                dups (->> (frequencies names)
                       (filter #(> (second %) 1))
                       (map first)
                       (apply sorted-set))]
            (if (empty? dups)
              result
              (throw (IllegalStateException.
                       (str "Command spec contains duplicates: " (s/join " " dups)))))))))))

(def init-command-spec
  [[:keep-alive? "Keep cluster alive after running the job." false]
   [:emr-name "The name for the cluster, as displayed in AWS console or listings."]
   [:enable-debugging? "Enable job flow debugging (results viewable in AWS Console)" true]
   [:master-instance-type "Instance type for master" "m1.large"]
   [:slave-instance-type "Instance type for slaves" "m1.large"]
   [:availability-zone "Amazon availabilty zone" nil]
   [:jobflow "A jobflow-id to use with the submit command" nil]
   [:num-instances "Number of instances (including the master)" "1"]
   [:job-flow-role "An IAM role for the job flow"]
   [:service-role "The IAM role that will be assumed by the Amazon EMR service
                   to access AWS resources on your behalf"]
   [:ami-version
    "Which AMI to use (see RunJobFlowRequest#setAmiVersion in the AWS Java SDK)"]
   [:endpoint
    "AWS API endpoint for non default regions. For example elasticmapreduce.eu-west-1.amazonaws.com"]
   [:subnet-id
    "VPC subnet id"]
   [:spot-task-group
    (str "Create a task group w/ spot instances. Value is type,price,num-addl-instances. "
         "E.g. m1.xlarge,30%,300")]
   [:runtime-jar
    (str "The hadoop job jar, should be an 's3://' path. Default "
         "is to use the jar uploaded by --jar-src-path.")]
   [:copy-runtime-jar?
    "If using runtime-jar, make a copy of it under jar-uri" false]
   [:jar-uri "S3 prefix for where the jar uri will be saved."]
   [:data-uri "S3 prefix for the data directory."]
   [:base-uri "S3 prefix for the base directory."]
   [:scripts-src-path
    (str "Local path with bootstrap scripts to be uploaded. To use them, you also need"
         "to specify :bootstrap-action.N options (see sample-jobdef.clj for details)")]
   [:jar-src-path
    (str "The local path of a JAR to upload. If specified "
         "this local jar file will be uploaded to S3.")]
   [:keypair "The Amazon keypair name."]
   [:user-packages "Any extra apt packages you want to install at startup"]
   [:show-progress? (str "Show progress bars for uploads on STDOUT. You may want "
                         "to disable when capturing the output to a log file.") false]
   [:comment "Arbitray text which is carried in the job metadata."]])
