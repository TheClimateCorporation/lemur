(ns com.climate.services.aws.s3
  (:use
    com.climate.services.aws.common)
  (:require
    [com.climate.io :as ccio]
    [clojure.tools.logging :as log]
    [clojure.java.io :as io]
    [clojure.string :as s])
  (:import
    org.apache.commons.codec.digest.DigestUtils
    [java.io InputStream ByteArrayInputStream File]
    org.apache.commons.io.IOUtils
    com.amazonaws.services.s3.AmazonS3Client
    [com.amazonaws AmazonServiceException
                   ClientConfiguration]
    [com.amazonaws.services.s3.model Bucket
                                     PutObjectRequest
                                     ProgressListener
                                     ProgressEvent
                                     ListObjectsRequest
                                     AmazonS3Exception
                                     ObjectListing
                                     ObjectMetadata
                                     S3ObjectSummary
                                     GetObjectRequest]))

; TODO should use the TransferManager com.amazonaws.services.s3.transfer for
;      async transfers and better performance

; TODO All functions that use this dynamic var should have an additional fn
;      signature where the object can be passed in explicitly
(def ^{:dynamic true} *s3* nil)

(defn s3-client [creds-object]
  (AmazonS3Client.
    creds-object
    (doto (ClientConfiguration.)
      ; Explicitly set some of the same values as the defaults
      (.setMaxErrorRetry 3)           ;default 3
      (.setSocketTimeout 10000)       ;default 50000 ms
      (.setConnectionTimeout 10000)   ;default 50000 ms
      (.setMaxConnections 50))))      ;default 50

(defn s3
  [creds]
  (aws s3-client creds))

(defn slash$?
  "Test if trailing slash exists"
  [s]
  (= (last s) \/))

(defn slash$
  [s]
  "Add trailing slash if it doesn't already exist."
  (if (slash$? s) s (str s "/")))

(defn slash!$
  "Remove trailing slash if it exists."
  [s]
  (if (= (last s) \/) (subs s 0 (dec (count s))) s))

(defn slash?
  "Test if leading slash exists"
  [s]
  (= (first s) \/))

(defn slash
  "Add leading slash if it doesn't already exist."
  [s]
  (if (slash? s) s (str "/" s)))

(defn slash!
  "Remove leading slash if it exists."
  [s]
  (if (= (first s) \/) (subs s 1) s))

(defn s3path
  "Create an s3 path, e.g. 's3://bucket/key'"
  [^String bucket ^String key]
  {:pre [bucket]}
  (if (seq key)
    (format "s3://%s/%s" bucket (slash! key))
    (format "s3://%s" bucket)))

(defn s3path?
  [s]
  (and s (string? s) (re-find #"^s3://" s)))

(defn parse-s3path
  "Parse the bucket and key from the given s3: path string.
  Returns [bucket key]"
  [s3path]
  (when s3path
    (let [parsed (re-find #"^s3://([^/]*)/?(.*)" s3path)]
      [(nth parsed 1) (nth parsed 2)])))

(defn buckets
  []
  (seq (.listBuckets *s3*)))

(defn bucket
  [bucket-like]
  (cond
    (instance? Bucket bucket-like) bucket-like
    :else (Bucket. bucket-like)))

(defn bucket-name
  "If given a String, returns it.  If given a Bucket object, returns the name of the bucket."
  [bucket-like]
  (cond
    (instance? Bucket bucket-like) (.getName bucket-like)
    :else bucket-like))

(defn bucket?
  [bucket-name]
  (.doesBucketExist *s3* bucket-name))

(defn create-bucket
  [bucket]
  (.createBucket *s3* bucket))

(defn delete-bucket
  [bucket-name]
  (.deleteBucket *s3* bucket-name))

(defn- objects*
  [req]
  (lazy-seq
    (let [listing (condp = (type req)
                    ListObjectsRequest (.listObjects *s3* req)
                    ObjectListing (.listNextBatchOfObjects *s3* req))
          objs [[(seq (.getCommonPrefixes listing))
                 (seq (.getObjectSummaries listing))]]]
      (if (.getNextMarker listing)
        (concat objs (objects* listing))
        objs))))

(defn objects
  "Returns [common-prefixes* S3ObjectSummaries*]
  Pass in nil delimiter for a recursive listing."
  ; An alternative implementation would have this function return just keys (similar to
  ; previous git revision); and another function to return "directories" (common-prefixes).
  ; But if you wanted both, that would require two REST calls, so this is more efficient.
  ([bucket-like]
    (objects bucket-like nil "/"))
  ([bucket-like prefix]
    (objects bucket-like prefix "/"))
  ([bucket-like prefix delimiter]
    (let [b (objects* (doto (ListObjectsRequest.)
              (.setBucketName (bucket-name bucket-like))
              (#(if prefix (.setPrefix % prefix)))
              (#(if delimiter (.setDelimiter % delimiter)))))
          f (fn [x y]
              [(concat (first x) (first y))
               (concat (second x) (second y))])]
      (reduce f b))))

(defn- object
  [bucket-like key]
  (when key
    (try
      (.getObject *s3* (bucket-name bucket-like) key)
      (catch AmazonS3Exception e
        (if-not (= 404 (.getStatusCode e))
          (throw e))))))

(defn object?
  [bucket-like key]
  (try
    (.getObjectMetadata *s3* (bucket-name bucket-like) key)
    (catch AmazonS3Exception e
      (if-not (= "Not Found" (.getMessage e))
        (throw e)))))

(defn delete-object
  ([object]
    (when object
      (.deleteObject *s3* (.getBucketName object) (.getKey object))))
  ([bucket-like key]
    (.deleteObject *s3* (bucket-name bucket-like) key)))

(defn mk-progress-listener
  [f state]
  (reify ProgressListener
    (progressChanged [this progress-event]
      (f state progress-event))))

(defn put-object-data
  "Puts the data to the specified bucket and key. streamable can be a InputStream,
  File, byte[], URL, or path String. If streamable is a File (or a path to a file),
  this function will determine the content-length for you.  Otherwise, if you don't
  specify content-length, the API will have to buffer the content in memory in order
  to calculate length. A checksum is calculated and verified after the data is
  uploaded. The bucket must already exist."
  [bucket-like ^String key streamable & {:keys [content-length progress-fn]}]
  (let [b (bucket-name bucket-like)
        content-length
          (or content-length
              (and (instance? File streamable) (.length streamable))
              (and (string? streamable) (-> streamable io/file .length)))
        stream (io/input-stream streamable)
        metadata (ObjectMetadata.)
        request (PutObjectRequest. b key stream metadata)]
    (if content-length (.setContentLength metadata content-length))
    (if progress-fn
      (let [state (progress-fn {:dest (s3path b key) :content-length content-length})]
        (.setProgressListener request (mk-progress-listener progress-fn state))
        (.putObject *s3* request)
        (progress-fn (assoc state :done true)))
      (.putObject *s3* request))))

(defn put-object-string
  "Puts the data to the specified bucket and key.
  The String data will be converted to a ByteArrayInputStream using the
  default encoding.  If this is unacceptable use `put-object-data` directly."
  [bucket-like ^String key ^String data]
  (let [bytes (.getBytes data)]
    (put-object-data bucket-like key bytes :content-length (count bytes))))

(defn copy-object
  "Copies the object from one S3 location to another S3 location. By default metadata
   is copied from the source object"
  [src-bucket src-key dest-bucket dest-key]
  (.copyObject *s3* src-bucket src-key dest-bucket
               (if (empty? dest-key) "/" dest-key)))

(defn- throw-if-not-404
  [f]
  (try (f)
       (catch AmazonS3Exception e
         (when (not= (.getStatusCode e) 404) (throw e)))))

(defn get-object-data
  "If dest arg is specified, download the given s3 object to it (dest can be a String
  or a File). If no dest, returns the contents of the s3 object as a byte array.  If
  etag (the md5 checksum as 32 digit hex) is supplied, then it is compared with the
  md5 digest of the dest.  If they are identical, the file is skipped.  Returns true
  if file is transferred, false if skipped."
  ([bucket-like key]
   (if-let [s3-obj (throw-if-not-404 #(.getObject *s3* (bucket-name bucket-like) key))]
     (->> s3-obj
       .getObjectContent
       (#(IOUtils/toByteArray #^InputStream %)))))
  ([bucket-like key dest]
   (let [dest (io/file dest)]
     (throw-if-not-404
       #(.getObject *s3* (GetObjectRequest. (bucket-name bucket-like) key) dest))))
  ([bucket-like key dest etag]
   (let [dest (io/file dest)]
     (if (and etag (.isFile dest) (= etag (DigestUtils/md5Hex (io/input-stream dest))))
       (do
         (log/debug (str "MD5 digests match, not transferring " key))
         false)
       (do
         (throw-if-not-404
           #(.getObject *s3* (GetObjectRequest. (bucket-name bucket-like) key) dest))
         true)))))

(defmulti cp
  "Copy files from a local path or s3 path to a local or s3 destination path (e.g.
  \"s3://bkt/foo/bar/\").  src and dest are Strings; s3 paths start with s3://.

  The semantics are similar to unix cp: if the src (relative or absolute) is a dir,
  then the directory is recursively copied to the dest.  If dest ends in a /, then
  the src files/dirs are copied with their original names, otherwise they are
  renamed (as per unix cp).

  If both src and dest are S3 paths, then this is a copy that will happen entirely
  in the AWS network.  Data is not streamed through your system.

  In the download case (i.e. an s3 src, and a local dest), it will check if the file
  exists locally, compare it's md5 digest, and if they match, skip the download."
  ^{:arglists '([src dest & args])}
  (fn [src dest & _]
    (vector (.startsWith (str src) "s3:")
            (.startsWith (str dest) "s3:"))))

; s3 to s3 copy
(defmethod cp [true true]
  [src dest & _]
  (let [dest (if (and (not (slash$? src)) (slash$? dest)) (str dest (ccio/basename src)) dest)
        [sbkt skey] (parse-s3path src)
        [dbkt dkey] (parse-s3path dest)]
    (if-not (object? sbkt skey)
      ; If src is a directory, then recursively copy to dest
      (let [lst (objects sbkt skey nil)
            f (fn [key skey dkey] (s/replace-first key (slash!$ skey) (slash!$ dkey)))]
        (->> lst
          second
          (map (memfn getKey))
          (map #(copy-object sbkt % dbkt (f % skey dkey)))
          doall))
      (copy-object sbkt skey dbkt dkey))))

(defn- new-s3-dest
  "Used by cp (for the s3 -> local case).  Determines the new path name, and
  creates the inetrmediate dirs."
  [^File dest parent-prefix key]
  (let [top (if (not (slash$? parent-prefix)) (ccio/basename parent-prefix))
        key-diff (s/replace-first key parent-prefix "")
        new-f (File. dest (str top key-diff))]
    (.. new-f getParentFile mkdirs)
    new-f))

;download
(defmethod cp [true false]
  [src dest & _]
  (let [[bkt orig-key] (parse-s3path src)
        dest (io/file dest)]
    (if-let [obj-meta (object? bkt orig-key)]
      (get-object-data
        bkt orig-key
        (if (.isDirectory dest) (File. dest (ccio/basename orig-key)) dest)
        (.getETag obj-meta))
      (let [lst (objects bkt orig-key nil)]
        (when (empty? lst)
          (throw (IllegalArgumentException. (format "No S3 object(s) found at s3://%s/%s" bkt orig-key))))
        (->> lst
          second
          (map (juxt (memfn getKey) (memfn getETag)))
          (map (fn [[key etag]]
                 (get-object-data
                   bkt key
                   (new-s3-dest dest orig-key key)
                   etag)))
          doall)))))

;upload
(defmethod cp [false true]
  [src dest & {:keys [progress-fn]}]
  (let [src-slash? (and (string? src) (slash$? src))
        src (io/file src)
        src-dir? (.isDirectory src)
        src-paths
          (if src-dir? (.listFiles src) [src])
        dest-path
          (cond
            (and src-dir? (not src-slash?) (slash$? dest))
              ; "foo" :to "bar/"  =>  foo to bar/foo/*
              (str dest (ccio/basename src) "/")
            src-dir?
              ; "foo/" :to "bar/" =>  foo/* to bar/*
              ; "foo" :to "bar"   =>  foo/* to bar/*
              ; "foo/" :to "bar"  =>  foo/* to bar/*
              (slash$ dest)
            :default
              ;  "foo/bar.txt" :to "bar/"         => bar.txt into the bar/ directory
              ;  "foo/bar.txt" :to "blah/bar.txt" => file is renamed
              dest)
        [dest-bkt dest-key]
          (parse-s3path dest-path)]
      (doseq [file src-paths]
        (if (.isDirectory file)
          (cp (.getCanonicalPath file) (slash$ dest-path))
          (put-object-data dest-bkt
            (if (slash$? dest-key) (str dest-key (ccio/basename file)) dest-key)
            file
            :progress-fn progress-fn)))))
