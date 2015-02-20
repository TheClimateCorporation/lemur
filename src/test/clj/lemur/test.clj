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

(ns lemur.test
  (:require
    [com.climate.services.aws [s3 :as s3] [common :as awscommon]]
    [clojure.tools.logging :as log]
    [clojure.test :as ct]))

(defmacro is=
  "Use inside (deftest) to do an equality test."
  ([form-left form-right] `(is= ~form-left ~form-right nil))
  ([form-left form-right msg] `(ct/try-expr ~msg (= ~form-left ~form-right))))

(defn with-bucket
  "Run the given function with a 'temp' bucket.  The bucket name must contain
  'unit'.  The bucket and contents will be deleted at the end of the test.
  Since the S3 bucket is a global resource, multiple people running the test at
  the same time could break the test. It's always safe to recursively delete the
  bucket and try again.
  The bucket is not deleted if an exception is thrown in the function. This is done
  so that you can debug the problem."
  [bkt f]
  (binding [s3/*s3* (s3/s3 (awscommon/aws-credentials))]
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
