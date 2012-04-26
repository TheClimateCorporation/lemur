(ns lemur.test
  (:require
    [com.climate.services.aws [s3 :as s3] [core :as awscore]]
    [clojure.tools.logging :as log]
    [lemur.common :as lc]
    [clojure.test :as ct]))

(defmacro is=
  "Use inside (deftest) to do an equality test."
  ([form-left form-right] `(is= ~form-left ~form-right nil))
  ([form-left form-right msg] `(ct/try-expr ~msg (= ~form-left ~form-right))))

(defn aws-creds
  []
  (awscore/load-credentials (lc/aws-credentials-discovery)))

(defn with-bucket
  "Run the given function with a 'temp' bucket.  The bucket name must contain
  'unit'.  The bucket and contents will be deleted at the end of the test.
  Since the S3 bucket is a global resource, multiple people running the test at
  the same time could break the test. It's always safe to recursively delete the
  bucket and try again.
  The bucket is not deleted if an exception is thrown in the function. This is done
  so that you can debug the problem."
  [bkt f]
  (binding [s3/*s3* (s3/s3 (aws-creds))]
    ; Sanity check to guard against an accidental recursive delete
    (if-not (.contains bkt "unit")
      (throw (IllegalArgumentException.
        "Unit tests must use a bucket name that includes the string 'unit'")))
    (s3/create-bucket bkt)
    (f)
    (doall (for [o (second (s3/objects bkt nil nil))]
      (do (log/info (format "deleting %s" (.getKey o)))
          (s3/delete-object o))))
    (s3/delete-bucket bkt)))
