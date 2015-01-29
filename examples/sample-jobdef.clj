;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Sample
;;; This job does nothing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; The symbols of lemur.core and lemur.command-line are
; automatically refer'ed in and the following namespaces are required for you:
;   clojure.java.io                :as io
;   clojure.string                 :as s
;   clojure.tools.logging          :as log
;   com.climate.shell              :as sh
;   lemur.util                     :as util
;   com.climate.services.aws.emr   :as emr
;   com.climate.services.aws.s3    :as s3
;   com.climate.services.aws.ec2   :as ec2

(comment ***
 Run "lemur help" for an overview and description of the concepts and
 syntax in this file.
 ***)

;;; Set the defaults

; Essentially, this does a Clojure (use), so all symbols from each base are automatically
; referred into your jobdef's namespace.  If this causes name collisions or you want more
; control, you can use require or other standard Clojure constructs (e.g. :only).
(use-base
  ; optional, lemur.base is always included automatically
  'your.org.base)

;;; Command line processing

;;; Catch args
; Additional command line options to accept.  You DO NOT need to specify
; options that ONLY your hadoop job expects-- use args.passthrough to let those
; options (i.e. :remaining) pass on to your hadoop job.  However, you may want to
; specify a fn to validate (:remaining eopts) before the hadoop job is triggered
; (see Custom options validation below).
; Each argument is either
;
;   1) a vector [:option-name doc-string default-value?]
;   where option-name is a keyword.  If the option is a boolean flag, it's name
;   should end with '?', and then no value will be expected on the command line,
;   default-value is optional; or
;
;   2) a pair of values, :option-name doc-string
;
;(catch-args
;  [:dummy "do nothing with this value" "dummy-default"]
;   :foo? "the foo flag"
;  [:bar "bar help text" (fn [eopts] (:dummy eopts))])
;
; The --bar example uses a fn for the default.  When the command line help is printed,
; the fn value is not very helpful to the user.  As an alternative, you can supply some
; text that should be used to document the default value. This can be done be putting
; :default-doc metadata on the function.  Here are two ways to do that:
;
;  1. [:bar "bar help text" (with-meta (fn [eopts] (:dummy eopts)) {:default-doc "same as dummy"})]
;  or
;  2. (defn ^{:default-doc "same as dummy"} barfn
;       []
;       (:dummy eopts))
;     and in (catch-args)
;       [:bar "bar help text" (pddd barfn)]
;     In the last line, pddd is necessary, b/c defn puts that metadat on the var, rather than the fn itself.
;
; Also note that you can specify default values in defcluster (see
; "Defaults for command line options").  A default in defcluster or a base will
; take precedence over a default in catch-args. BUT this default will NOT appear
; in the help documentation printed for "lemur help path/to/jobdef.clj"
;
; Also note that args defined in your defstep are automatically added to catch-args

;;; Profiles

; Enable the profile :foo, which means that values in the nested map :foo will
; override other entries.  Generally, profiles would be enabled on the command
; line, but it is possible to do it in the jobdef like this.
;(add-profiles [:foo])

;;; Custom options validation

; OPTIONAL. A set of functions that are run before your job is started to validate the
; command line options, environment or anything else you like.
;
; Each function is either a 0-arg function or a 1-arg function.  In the latter case, eopts
; is passed in.  eopts includes all your values in defcluster, the command line options,
; defaults, values from bases, etc).  The function returns a String, vector of Strings
; or false on failure (the strings will be output as the failure message).  On success
; the fn should return nil or true or '().
;
; You can write your own functions for arbitrary checks (consider using the helper
; functions: lemur.core/lfn, and/or lemur.common/mk-validator). However, for many common
; cases, you can use lemur.common/val-opts and lemur.common/val-remaining; which provide
; a declarative method for specifying validations.
;
; RECOMMENDED defining validators can save time by avoiding a cluster launch that fails
;             because of missing/bad options. In particular, remember to write a validator
;             to check :remaining. Any options that are not caught are left in remaining,
;             so if someone mis-types an option it could show up here.
;
; EXAMPLES
;(add-validators
;  (lfn [dataset]
;    (if-not
;      (contains? #{"ahps" "stage_iv"} dataset)
;      "--dataset must be specified as 'ahps' or 'stage_iv'"))
;  (val-opts :file :days-file)
;  (val-opts :required :numeric :num-days))

;;; Hooks (actions)

; While you can include arbitrary Clojure code anywhere, use hooks for a more
; structured approach to executing actions as part of your job.
;(add-hooks
;  [optional-boolean-expr] function-to-execute
;  ...
;  )

;;; Define cluster

; See note above on map values
(defcluster sample-cluster

  ;; DEFAULT values
  ; The defaults from (base) are shown. If you include other files in (use-base)
  ; then they might change the defaults. However, if the option has no default
  ; (as noted below), then an example is shown.

  ; A symbolic name for this job.
  ; The default is the cluster name specified as the first arg to defcluster
  ;:app "${jobdef-file}"

  ; The name of the cluster (aka jobflow), visible in AWS console or elastic-mapreduce listings
  ;:emr-name "${run-id}"

  ; Enable this feature, to save the details of the job (options, bootstrap-actions,
  ; steps with their args, uploads, args, etc) to "${base-uri}/METAJOB.yml".
  ;:metajob-file true

  ; The bucket name in s3 where emr logs, data, scripts, etc will be stored.
  ;:bucket "com.your-co.${env}.hadoop"

  ; A local path where your jar exists. If specified, it will be uploaded to S3 automatically.
  ; REQUIRED.
  ;:jar-src-path "build/your-hadoop-java.jar"
  ;:ec2 {:jar-src-path "/location/of/your/deployed/hadoop-java.jar"}

  ; An s3 path for the hadoop job jar. Takes precedence over jar-src-path.
  ; NO DEFAULT
  ;:runtime-jar "s3://com.your-co.${env}.hadoop/marc/my-hadoop-java.jar"

  ;Number of instances (including the master)
  ;:num-instances 1

  ;Instance type for slaves
  ;:slave-instance-type "m1.large"

  ;Instance type for master
  ;:master-instance-type "m1.large"

  ;EMR Cluster tags
  ;Provide comma-separated list of key=value pairs, which will be used to
  ;create tags for instances in your EMR cluster
  ;read more at
  ;http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-plan-tags.html
  ;:tags "fake:project=fightingcrime,fake:group=ateam"
  ; Uncomment to attempt to get additional nodes via the spot market
  ; This example requests up to 30 additional m1.xlarge nodes, and we are willing to
  ; pay up to 80% of the difference between the reserve price and the demand price.
  ; NO DEFAULT.  Example:
  ;:spot-task-group "m1.xlarge,80%,30"

  ; To keep the cluster alive after running the job steps, change this to true.
  ; In particular, if your job is failing, and you want to debug on the live cluster,
  ; set this option to true.
  ;:keep-alive? false

  ; The keypair file to use (this is just a short name, not a pathname)
  ; EXAMPLE
  ;:keypair "your-keypair"

  ; Set true to enable debugging, which simply indexes the log files
  ; so they can be accessed from the AWS console.  No impact on performance.
  ;:enable-debugging? true

  ; To enable ec2 monitoring (basic stats are collected and viewable in the AWS console), set to true
  ;:enable-ec2-monitoring false

  ; The location of the bootstrap action scripts (a local path).  All scripts in this
  ; directory will be uploaded to S3.
  ; REQUIRED
  ;:scripts-src-path "location/of/your/bootstrap-scripts"
  ;:ec2 {:scripts-src-path "/deployed/location/of/your/bootstrap-scripts"}

  ; run-path is used to determine the base-uri
  ;:run-path "${run-id}"
  ; To use the same paths every time, try this:
  ;:run-path "${app}/shared-data"

  ; The base-uri is the S3 root path for all things associated with the current run.
  ;:base-uri "s3://${bucket}/runs/${run-path}"
  ;:local {:base-uri "/${ENV.HOME}/lemur/${run-path}"}

  ; The S3 location where the cluster logs will be stored (AFTER CLUSTER SHUTDOWN)
  ;:log-uri "${base-uri}/emr-logs"

  ; The S3 prefix where data should be stored. If :args.data-uri is true, this path will be passed
  ; as an argument to your hadoop job. It is intended to be the output path for any generated data
  ; that should be persistent.
  ;:data-uri "${base-uri}/data"

  ; The location under base-uri where your uploaded bootstrap-action scripts will be saved.
  ; REQUIRED
  ;:std-scripts-prefix "runs/${run-id}/emr-bootstrap"

  ; Uploads
  ; Specify files that should be uploaded from your local fs to s3 (or s3 to s3, or local to local).
  ; Each file can be absolute or relative, and can name a file or a directory (directories are processed
  ; recursively). The default destination is ${data-uri} (also note that this works in local mode,
  ; since $(data-uri} is automatically set to a local path in that case). If you want to specify a different
  ; destination, follow your source path by :to and then another string which is the dest.  Again, this dest
  ; string can be relative or absolute, and can be a local path or an s3 path.  If it is not an absolute
  ; path, than it is considered relative to ${data-uri} (i.e. "the remote working directory").
  ; NO DEFAULT.  Examples:
  ;:upload ["file1" "/tmp/file2"]
  ; Results in
  ;   - file1 from your CWD uploaded to ${data-uri}/file1
  ;   - /tmp/file2 uploaded to ${data-uri}/file2
  ;:upload ["file"
  ;         "bar/file" :to "COUNTIES"
  ;         ;in the next line, the / at the end of cropy is significant, see below
  ;         "/tmp/input-dir" :to "cropy/"
  ;         Next one is [src dest]. Useful if you're constructing the structure from a function
  ;         ["./foo.txt" "data.txt"]
  ;         "/tmp/input-dir" :to "s3://${bucket}/foo"]
  ; Results in
  ;   - file from your CWD uploaded to ${data-uri}/file
  ;   - bar/file from your CWD uploaded to ${data-uri}/COUNTIES
  ;   - /tmp/input-dir uploaded to ${data-uri}/cropy/input-dir
  ;     input-dir is copied under cropy b/c of the trailing slash above
  ;   - ./foo.txt uploaded to ${data-uri}/data.txt
  ;   - /tmp/input-dir uploaded to "s3://${bucket}/foo"

  ; Enable this feature, so that uploads to S3 will display an ascii progress bar during
  ; the transfer to give you an indication of how long the upload will take.
  ;:show-progress? true

  ; Additional bootstrap actions can be added by using a key of the form
  ; :bootstrap-action.N
  ; Where N is a unique integer which indicates the order in which the scripts should
  ; be executed. Use "lemur dry-run" to display the BAs that are currently set.
  ; The value can be (literally or as the result of a fn) any of:
  ; - nil
  ;   do nothing (maybe as the result of a fn with a conditional; or as a way to
  ;   skip a bootstrap-action defined in a base)
  ; - [ba-name script-name-or-path args]
  ;   ba-name is an arbitrary string used to label the script when it is run
  ;   script-name-or-path is either a simple name like "my-config.sh" or a full path
  ;   starting with "s3://". If you supply a name only, it will be looked for under
  ;   :std-scripts-prefix of the :bucket.  See :scripts-src-path above, generally
  ;   anything in that location can just be referred to by name.
  ;   args is a vector of string arguments for the BA script
  ; - [ba-name script-name-or-path]
  ;   as above, but no arguments for the script
  ; - [script-name-or-path]
  ;   as above, but ba-name will be "Custom Config"
  ; Example:
  ;:bootstrap-action.N "s3://path/to/script"

  ; Modify the hadoop configuration. Any key starting with ":hadoop-config." is
  ; concatenated to the default config (see :hadoop-config.* in
  ; lemur.base/update-base). The part of the key following the dot is ignored,
  ; but serves to avoid overlap with hadoop-config keys from any included base.
  ; Use "lemur dry-run <job-def.clj>" to see the current hadoop-config. keys.
  ;
  ; The value is a collection of strings, which are the args passed to Amazon's hadoop
  ; bootstrap-action script:
  ;   http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuide/index.html?Bootstrap.html#PredefinedBootstrapActions_ConfigureHadoop"
  ; In short, the config consists of pairs: the first entry is -c, -m or -h and indicates the
  ; hadoop config file that should be modified.
  ;  "-c" => "/home/hadoop/conf/core-site.xml"
  ;  "-h" => "/home/hadoop/conf/hdfs-site.xml"
  ;  "-m" => "/home/hadoop/conf/mapred-site.xml"
  ; And the second is a name=value to add to the file.
  ;
  ; NOTE: since this is implemented via a Bootstrap Action, it has no impact in local mode.
  ;
  ; For example, the entry below sets the max map tasks to 7.
  ;:hadoop-config.custom ["-m" "mapred.tasktracker.map.tasks.maximum=7"]

  ; For local mode only, to set ENVIRONMENT VARIABLES for Hadoop, you can
  ; specify a Map of name value like this.
  ; EXAMPLE
  ;:local {:hadoop-env {"HADOOP_HEAPSIZE" "2048"}}

  ; To run cluster in non default region, specify AWS API endpoint
  ; (list of endpoints http://docs.aws.amazon.com/general/latest/gr/rande.html#emr_region)
  ;:endpoint "elasticmapreduce.eu-west-1.amazonaws.com"
  ; To run cluster in VPC subnet, specify subnet's id
  ;:subnet-id "subnet-b35104f5"
  )


;;; Define one or more steps

(defstep sample-step

  ; REQUIRED The classname for the class with the main function, i.e. the Hadoop
  ; job entry point
  :main-class "com.your-co.some.Class"

  ; A symbolic name given to the step. This is what is displayed in a jobflow
  ; listing and the AWS console.
  ; DEFAULT is the name given to defstep minus '-step' suffix if it exists.
  ;:step-name "some other name"

  ; The jar to use for this step.
  ; DEFAULT the jar specified in :runtime-jar or :jar-src-path, but this JAR is
  ; not copied under the base-uri path.
  ;:step-jar "s3://bkt/foo/bar.jar"

  ; Defaults for command line options
  ; Same as for defcluster (but limited in scope to the step)

  ; See the docs on "JOB ARGS" in help.txt
  ; :args.data-uri is RECOMMENDED. data-uri is important if you use local
  ; mode, as the value of :data-uri is adjusted for that purpose.
  ; Also note that args like :foo and :bar? below would be automatically added
  ; to (catch-args):
  ;:args.foo "some value"
  ;:args.bar false
  ;:args.positional ["foo" "bar"]
  :args.passthrough true
  :args.data-uri true

  )

;;; Fire! (i.e. start the cluster and run the steps)

; fire! returns right away. The jobflow-id is saved (context-get :jobflow-id).
; (fire! cluster steps)
; steps is a list (in-line or collection) of steps to run, or a "fn of eopts",
; which returns a step (created by defstep or a StepConfig object) or a collection of steps.
; cluster is defined by a previous defcluster, or a fn that returns a single cluster.
(fire! sample-cluster sample-step)

; If you want to block on cluster startup, where <stage> is one of
;  :provisioned
;  :accepted
;  :ready
;(wait <stage>)

; If you want to block on step completion use wait-on-step as below;
; returns a map with information on how the job completed.  If the timeout expires it might look like
;    {:timeout true, :success false}
; if the job completes before the timeout expires it might look like
;    {:success true}
; Read the function documentation for more details.
;
;(wait-on-step step timeout-seconds)
