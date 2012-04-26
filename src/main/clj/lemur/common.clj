(ns lemur.common
  "Provides canned functions which are useful for common use-cases. These functions
  are not relied upon by core lemur (except for the core base), but may be used in
  your own jobdefs."
  (:use
    [lemur.core :only [lfn]]
    [lemur.command-line :only [extract-cl-args quit]])
  (:require
    [clojure.string :as s]
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [com.climate.shell :as sh]
    [com.climate.io :as ccio]
    [lemur.util :as util]
    [com.climate.services.aws.s3 :as s3]
    [com.climate.services.aws.emr :as emr])
  (:import
    java.io.IOException
    java.io.File))

;;; Validators

;TODO make a more extensible model for adding new checks
;     (e.g. the check name, :required, can be looked up in a map of functions)
(defn val-opts
  "Creates a validator function, which validates named options in eopts.

    (val-opts options+ [keyword-name*] err-msg?)

  options is one or more of:

    :required - this option is required
    :numeric - the option, if it exists, must be numeric (float or int)
    :word - the option, if it exists, must contain only word characters (alpha-numeric, _, -)
    :file - the option, if it exists, must be an existing file (local or S3)
    :dir - the option, if it exists, must be an existing dir (local or S3)
    :file-or-dir - the option, if it exists, must be an existing file/dir (local or S3)
    :local-dir - the option, if it exists, must be an existing local directory

  A single keyword-name, or a collection of keyword-names follows the options.
  These are the keywords in eopts that you want to validate.

  err-msg is an optional String with a custom error message to report on failure.
  If err-msg is not given, than a suitable message is constructed based on the
  check that failed.

  Examples:

    ; The :keypair option is required. In all cases, :required can be satisified
    ; either by an entry in the jobdef or with a --option on the command-line
    (val-opts :required [:keypair])

    ; :scripts-src-path is required, and must be an existing file/directory
    (val-opts :dir :required [:scripts-src-path])

    ; :some-optional-path is NOT required, but if specified, it must be
    ; an existing file/directory
    (val-opts :file-or-dir [:some-optional-path])

    ; Both :app and :foo are required, and must contain only word-characters
    (val-opts :required :word [:app :foo])"
  [& args]
  (let [[args err-msg]
          (if (-> args last string?)
            [(butlast args) (last args)]
            [args nil])
        err (fn [k v msg] (format "[%s = %s] %s" k v (or err-msg msg)))
        opts (butlast args)
        keywords (util/collectify (last args))
        required? (some #{:required} opts)
        file? (some #{:file} opts)
        dir? (some #{:dir} opts)
        file-or-dir? (some #{:file-or-dir} opts)
        local-dir? (some #{:local-dir} opts)
        numeric? (some #{:numeric} opts)
        word? (some #{:word} opts)]
    (fn [eopts]
      (flatten
        (for [k keywords
              :let [v (get eopts k)]]
          (filter identity (vector
            (when required?
              (if-not v (err k v "required option not specified")))
            (when (and v file?)
              (if-not (util/file-exists? v)
                (err k v "file could not be found")))
            (when (and v dir?)
              (if-not (util/dir-exists? v)
                (err k v "dir could not be found")))
            (when (and v file-or-dir?)
              (if-not (util/file-or-dir-exists? v)
                (err k v "file/dir could not be found")))
            (when (and v local-dir?)
              (if-not (and (util/dir-exists? v) (not (s3/s3path? v)))
                (err k v "path could not be found or is not a local directory")))
            (when (and v numeric?)
              (if-not (or (number? v) (re-find #"^-?(\d*\.\d+|\d+)$" v))
                (err k v "must be numeric")))
            (when (and v word?)
              (if-not (re-find #"^[-\w]+$" v)
                (err k v "must contain only word characters (0-9, a-z, A-Z, - or _)"))))))))))

(defn val-remaining
  "Creates a validator function. Validates the remaining args (i.e. those not
  caught by lemur or your jobdef).

    (val-remaining pred* err-msg?)

  Specify one or more predicates from the following:

    :min N - should contain at least N entries
    :max M - should contain at most M entries
    :empty true - should contain exactly 0 entries
    :required coll - the collection lists required options in --opt form (requires
                     that :mini-spec, described below, be specified)

  and optionally specify

    :mini-spec - A mini command spec, which, if it exists, is applied before the
    checks above, so that those checks are only validating what is 'left-over'.
    It's value is a collection of keywords naming options that are understood by
    your hadoop main-class, boolean options should be indicated with a ? at the
    end (e.g. :bar?).

  err-msg is an optional String with a custom error message to report on failure.
  If err-msg is not given, than a suitable message is constructed based on the
  check that failed.

  Examples

    ; There are at least 2, but no more than 5 remaining args
    (val-remaining :min 2 :max 5)

    ; The remaining args may optionally contain '--foo value' and '--bar' (the latter
    ; with no trailing value, since it is identified as a boolen option). Disregading
    ; these options, the rest of remaining should be empty.
    (val-remaining :mini-spec [:foo :bar?] :empty true)

    ; Like the previous example a number of options that may be specified, and the
    ; rest should be empty.  But it also specifies that option '--foo value' is required.
    (val-remaining :mini-spec [:foo :bar? :baz] :required [:foo] :empty true)"
  [& args]
  (let [[args err-msg]
          (if (-> args last string?)
            [(butlast args) (last args)]
            [args nil])
        err (fn [rargs ropts fmt & fmt-args]
              (apply format (str "Remaining [%s %s]: " (or err-msg fmt)) rargs ropts fmt-args))
        {:keys [mini-spec min max empty required]
         :or {mini-spec [] min 0 max Integer/MAX_VALUE empty false required []}}
          args]
    (when (and (seq required) (empty? mini-spec))
      (quit :exit-code 1 :msg "(val-remaining) call has a :required clause, but not a :mini-spec"))
    (lfn [remaining]
      (let [[remaining-opts remaining-args] (extract-cl-args mini-spec remaining)
            results (vector
                      (if-not (>= (count remaining-args) min)
                        (err remaining-opts remaining-args "Must contain at least %s elements" min))
                      (if-not (<= (count remaining-args) max)
                        (err remaining-opts remaining-args "Must not contain more than %s elements" max))
                      (if (and empty (seq remaining-args))
                        (err remaining-opts remaining-args "Should be empty"))
                      (for [r required :when (not (get remaining-opts r))]
                        (err remaining-opts remaining-args
                             "Does not contain required option %s"
                             (str "--" (s/replace (name r) #"\?$" "")))))]
        (->> results flatten (keep identity))))))

(defn mk-validator
  "Creates a validator function from a predicate. (pred eopts) should return a falsey
  value on failure, in which case err-msg will be used for the error string."
  [pred err-msg]
  (fn [eopts] (if-not (pred eopts) err-msg)))

;;; Steps

(util/defalias debug-step-config emr/debug-step-config)

(defn mk-resize-spot-task-group-step
  "A Step which can be used to resize the TASK spot instance group."
  [instance-count]
  {:step-name "resize-spot-task-group"
   :args-order [:args.modify-instance-group :args.instance-count]
   :step-jar "s3://us-east-1.elasticmapreduce/libs/resize-job-flow/0.1/resize-job-flow.jar"
   :args.modify-instance-group "task"
   :args.instance-count (str instance-count)})

;;; Hooks

(defn- diff-parts
  "Do a diff on the sorted part files.
  WARNING This fn makes use of the following system utilities:
    cat, sort, diff
  Returns the result of sh/sh, which is a map."
  [left right]
  (let [[leftf rightf]
        (for [path [left right]]
          (let [file (File/createTempFile "diff-parts-" nil)
                exit (:exit (sh/sh "bash" "-c" (format "cat %s/part-* | sort" path) :err :pass :out file))]
            (if-not (zero? exit)
              (throw (RuntimeException. (format "Non-zero exit code %d for %s" exit path))))
            file))]
    (sh/sh "diff" "-u" (.getPath leftf) (.getPath rightf) :out :pass)))

(defn- quit-on-failure
  "First arg is the result of com.climate.shell/sh (a map). If the sh result
  indicates a failure, via it's :exit value, then quit with the given msg for
  output. The exit-code of the process will be the :exit value from the map."
  [sh-result msg]
  (when-not (zero? (:exit sh-result))
    (quit :msg msg :exit-code (:exit sh-result))))

(defn diff-test-data
  "Validate test data against known expected results. Creates a fn suitable for
  use with add-hooks. Typically it would also be guarded by (when-local-test).
  For example:

    (add-hooks
      (when-local-test)
        (diff-test-data [\"SOME-NAME\" \"path/relative/to/test-uri\"]
                        ...))

  Two options from eopts are used, :data-uri and :test-uri. The output of your
  job should be in a tree under ${data-uri}/ and your expected results in a tree
  under ${test-uri}/expected/

  Each entry in tests is a vector [name path].  Where name is an arbitrary string
  for display purposes, and path is a directory path relative to both ${data-uri}/
  and ${test-uri}/expected/.

  If any one of the tests fails, validation will stop at that point and the process
  will quit with a non-zero exit-code. An order independent comparison is done
  with your system's sort and diff utilities."
  [& tests]
  (fn [eopts _]
    (printf "Comparing expected (%s/expected) to results (%s)%n"
            (:test-uri eopts) (:data-uri eopts))
    (doseq [[name path] tests]
      (quit-on-failure
        (diff-parts
          (str (:test-uri eopts) "/expected/" path)
          (str (:data-uri eopts) "/" path))
        (str "***** FAILURE ON " name " *****")))
    (println "***** SUCCESS *****")))

;;; Discovery

(defn aws-credentials-discovery
  "Attempt to discover the path of your AWS credentials in JSON format.
     - Check for `which elasic-mapreduce`/credentials.json.
     - TODO look alongside jobdef
     - TODO look in PWD
     - TODO in tools/lemur, basedir/credentials.json"
  []
  (let [emr-dir (try (sh/sh "which" "elastic-mapreduce")
                     (catch IOException iox
                       (log/trace iox "Not able to find elastic-mapreduce cli.")))
        path (io/file
               (-> emr-dir :out s/trim ccio/dirname)
               "credentials.json")]
    (if (.exists path)
      path
      (log/warn "Unable to locate credentials.json. Try installing"
                "elastic-mapreduce cli and make sure it is in your PATH."))))
