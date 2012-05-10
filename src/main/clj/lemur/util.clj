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

(ns lemur.util
  (:use
    [clojure.pprint :only [cl-format]])
  (:require
    [clojure.string :as s]
    [clojure.java.io :as io]
    [com.climate.io :as ccio]
    [clojure.tools.logging :as log]
    clj-http.client
    [com.climate.yaml :as yaml]
    [com.climate.services.aws.s3 :as s3])
  (:import
    [org.yaml.snakeyaml Yaml]
    java.lang.ProcessBuilder
    java.io.File
    java.text.SimpleDateFormat
    java.util.Date
    java.util.TimeZone))

; We define an additional method below on the s3/cp multimethod
(def cp s3/cp)

(defn time-str
  "The current time (UTC) as a string. Default format is yyyy-MM-dd-HHmm"
  ([]
    (time-str "yyyy-MM-dd HH:mm:ss"))
  ([time-format]
    (let [sdf (doto (SimpleDateFormat. time-format)
                    (.setTimeZone (TimeZone/getTimeZone "UTC")))]
      (.format sdf (Date.)))))

(defn as-int
  "Coerce the argument to an int"
  [x]
  (cond
    (nil? x) 0
    (string? x) (Integer/parseInt x)
    :default (int x)))

(defn uuid
  ([] (str (java.util.UUID/randomUUID)))
  ([n] (subs (uuid) 0 n)))

(defn shortname
  [filename]
  (-> filename
    (s/replace #"[-_]?jobdef\.clj|\.clj$" "")
    (->> (re-find #"[^/]+$"))))

(defn print-map
  [title m]
  (println title)
  (when (seq m)
    (let [m (sort (filter val m))
          longest (apply max (map #(-> % key name count inc) m))]
      (cl-format true (str "~:{~2T~" longest "a ~a~%~}") m)))
  (println ""))

(defmethod cp [false false]
  [src dest]
  ; Used when src and dest are both local (not s3 paths)
  ; Copy files or directories (recursively). src and dest can be absolute or relative;
  ; and can be string paths, File objects or URLs.
  (ccio/cp src dest))

(defn mk-absolute-path
  "Get an absolute path. If x is already absolute, return it. If not, prepend
  working-dir to it.  If working-dir is nil, it defaults to the CWD.  Both
  x and working-dir can be Strings or File objects, but the return value is
  always a String."
  [working-dir x]
  (let [x (or x "")
        file? (instance? File x)
        working-dir (or working-dir (System/getProperty "user.dir"))]
    (cond
      (and file? (not (.isAbsolute x)))  (.getPath (File. (io/file working-dir) (.getPath x)))
      (and file? (.isAbsolute x))        (.getPath x)
      (s3/s3path? x)                     x
      (s3/slash? x)                      x
      ; no slash?, and s3 wd
      (s3/s3path? working-dir)           (str (s3/slash$ working-dir) x)
      ; no slash?, and local wd
      :default                           (.getPath (File. (io/file working-dir) x)))))

(defn progress-meter
  "pct is a number between 0 and 1."
  [pct length]
  {:pre [(>= pct 0.0) (<= pct 1.0)]}
  (let [dot-length (int (* pct length))
        space-length (- length dot-length) ]
    (apply str
      (concat "[" (repeat dot-length "*")
              (repeat space-length " ") "]"
              (repeat (+ length 2) "\010")))))

(defn- upload-progress-fn
  ([progress-bar-length state]
   (if (:done state)
     (do
       ;finish
       (println (progress-meter 1 progress-bar-length))
       (flush))
     (do
       ;init
       (println (format "Uploading %s:" (:dest state)))
       (print " " (progress-meter 0 progress-bar-length))
       (flush)
       (assoc state :total-bytes-xfered (atom 0)))))
  ([progress-bar-length {:keys [total-bytes-xfered content-length]} event]
   (swap! total-bytes-xfered + (.getBytesTransfered event))
   (print (progress-meter
            (if (zero? content-length) 1 (/ @total-bytes-xfered content-length))
            progress-bar-length))))

(defn upload
  "Upload the local files to S3 (or a local destination).  xfer-directives is a coll of tuples,
  where each tuple is [src] or [src dest].  src can be a file or a directory, and can be
  an absolute or relative path.  dest is an absolute path (starting with
  \"s3://bkt/..\"), or a path which is relative to dest-working-dir.  If dest
  ends in a /, the files are uploaded (recursively) with their original names
  under the dest prefix.  If dest does not end with /, then the files are uploaded
  (recursively) and renamed as per unix cp.  Examples, where S3WD is the
  dest-working-dir:

    [\"foo\" \"bar\"]  =>  S3WD/bar
    [\"foo\" \"bar/\"]  =>  S3WD/bar/foo
    [\"foo\" \"s3://bkt/bar/\"]  =>  s3://bkt/bar/foo

  For a user-friendly experience, set show-progress? to true in order to display
  ascii progres bars indicating how much of each transfer has been completed
  (applies only for 'local -> S3' uploads)"
  ([show-progress? xfer-directives]
   (upload show-progress? xfer-directives nil))
  ([show-progress? xfer-directives dest-working-dir]
   (when (and dest-working-dir (not (s3/s3path? dest-working-dir)))
     (.mkdirs (io/file dest-working-dir)))
   (doseq [[src dest] xfer-directives]
     (let [abs-dest (mk-absolute-path dest-working-dir dest)]
       (log/infof "upload %s to %s" src abs-dest)
       (if (and show-progress? (not (s3/s3path? src)) (s3/s3path? abs-dest))
         (cp src abs-dest :progress-fn (partial upload-progress-fn 50))
         (cp src abs-dest))))))

(defn mk-yaml
  [name data opts]
  (binding [yaml/*yaml* (Yaml. opts)]
    (yaml/generate-string {name data})))

(defn file-exists?
  "Tests that a path exists. path is either a path to a local file, or an S3
  path (of an object) starting with s3://"
  [path]
  (if (.startsWith path "s3:")
    (apply s3/object? (s3/parse-s3path path))
    (.isFile (io/file path))))

(defn dir-exists?
  "Tests that a path exists. path is either a path to a local directory, or an S3
  path (of a 'directory') starting with s3://"
  [path]
  (if (.startsWith path "s3:")
    (->> path s3/slash!$ s3/parse-s3path (apply s3/objects) first seq)
    (.isDirectory (io/file path))))

(defn file-or-dir-exists?
  [path]
  (or (file-exists? path) (dir-exists? path)))

(defn data-path
  "Returns the basename of x appended to ${data-uri}.  This is useful after using the :upload
  directive (without :to) to get the new path."
  [x]
  (str "${data-uri}/" (ccio/basename x)))

(defn arities [f]
  ;copied from https://groups.google.com/forum/?fromgroups#!topic/clojure/2ZU62kgGjXg
  ;WARNING: this is a bit of a hack. Future changes to Clojure around invoke might break this fn.
  (let [methods (.getDeclaredMethods (class f))
        count-params (fn [m]
                        (->> methods
                          (filter #(= m (.getName %)))
                          (map #(count (.getParameterTypes %)))))
        invokes (count-params "invoke")
        do-invokes (map dec (count-params "doInvoke"))
        arities (sort (distinct (concat invokes do-invokes)))]
    (if (seq do-invokes)
      (concat arities [:more])
      arities)))

(defn has-arity?
  "True if f has a signature with arity n"
  [f n]
  (some #{n} (arities f)))

(defmacro defalias
  ; copied from clojure.contrib.def (b/c I don't see a port of that lib to clj 1.3)
  "Defines an alias for a var: a new var with the same root binding (if
  any) and similar metadata. The metadata of the alias is its initial
  metadata (as provided by def) merged into the metadata of the original."
  ([name orig]
     `(do
        (alter-meta!
         (if (.hasRoot (var ~orig))
           (def ~name (.getRawRoot (var ~orig)))
           (def ~name))
         ;; When copying metadata, disregard {:macro false}.
         ;; Workaround for http://www.assembla.com/spaces/clojure/tickets/273
         #(conj (dissoc % :macro)
                (apply dissoc (meta (var ~orig)) (remove #{:macro} (keys %)))))
        (var ~name)))
  ([name orig doc]
     (list `defalias (with-meta name (assoc (meta name) :doc doc)) orig)))

(defn collectify
  "Coerce the arg into a collection. If x is nil, returns []."
  [x]
  (cond
    (nil? x) []
    (map? x) [x]
    (coll? x) x
    :default (vector x)))

; Copied from clojure.core/merge-with, but the merge fn takes an extra arg: the key
(defn merge-with-key
  "Returns a map that consists of the rest of the maps conj-ed onto
  the first.  If a key occurs in more than one map, the mapping(s)
  from the latter (left-to-right) will be combined with the mapping in
  the result by calling (f key val-in-result val-in-latter)."
  [f & maps]
  (when (some identity maps)
    (let [merge-entry (fn [m e]
                        (let [k (key e) v (val e)]
                          (if (contains? m k)
                            (assoc m k (f k (get m k) v))
                            (assoc m k v))))
          merge2 (fn [m1 m2]
                   (reduce merge-entry (or m1 {}) (seq m2)))]
      (reduce merge2 maps))))

(defn lemur-merge
  "Verify the keys being merged to make sure that bootstrap-actions do
  not overlap unless the value is nil. This fn is intended to avoid unintentional
  overrides (without being explicit) when merging maps."
  [& maps]
  (apply merge-with-key
    (fn [k v1 v2]
      (cond
        (re-find #"^bootstrap-action\." (name k))
          (if (or (nil? v2) (nil? v1))
            v2
            (throw (IllegalStateException. (format
                "Can not override %s unless specifying nil, old %s, new %s"
                k v1 v2))))
        (= :remaining k)
          (throw (IllegalStateException. (format
                       (str "Key :remaining is reserved and can not be set."
                            "Attempt was made with the values '%s' and '%s'")
                       v1 v2)))
        (and (= :upload k) (not (fn? v2)))
          ; In particular, this conditional allows :upload to be overridden
          ; (rather than concatenated) if the new value is a fn
          (concat (collectify v1) (collectify v2))
        (= :display-in-metajob k)
          (concat (collectify v1) (collectify v2))
        (and (map? v1) (map? v2))
          (merge v1 v2)
        :default
          v2))
    maps))

(defn fixed-wait
  [n]
  (let [time-ms (* 1000.0 n)]
    (log/debug (format "Sleeping for %.0f ms" time-ms))
    (Thread/sleep time-ms)))

(defn fixed-retry-until
  "Call f until retry-pred, up to max-attempts times.
  retry-pred is called with a single arg: either the result of calling f
  or the exception is thrown. Waits interval-seconds between each try."
  ([f max-attempts interval-seconds]
     (fixed-retry-until f max-attempts #(not (instance? Throwable %))))
  ([f max-attempts interval-seconds retry-pred]
     (let [result (try (f) (catch Throwable t t))
           retry? (not (retry-pred result))]
       (if retry?
         (if (zero? max-attempts)
           ; max-attempts used up, so throw Exception if there is one, otherwise return nil.
           (when (instance? Throwable result) (throw result))
           ; try again...
           (do
             (log/debug (str max-attempts " retries remaining"))
             (fixed-wait interval-seconds)
             (recur f (dec max-attempts) interval-seconds retry-pred)))
         result))))
