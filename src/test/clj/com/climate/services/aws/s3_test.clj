(ns com.climate.services.aws.s3-test
  (:use
    com.climate.services.aws.s3
    lemur.test
    clojure.test)
  (:require
    [clojure.tools.logging :as log])
  (:import
    java.io.File
    com.google.common.io.Files))

(def test-bucket "lemur.unit.s3")

(use-fixtures :once
  (fn [f] (binding [*s3* (s3 (aws-creds))] (f))))

(deftest test-slash+derivitives
  (is= [false "foo/" "foo" false "/foo" "foo"]
       ((juxt slash$? slash$ slash!$ slash? slash slash!) "foo"))
  (is= [true "foo/" "foo"]
       ((juxt slash$? slash$ slash!$) "foo/"))
  (is= [true "/foo" "foo"]
       ((juxt slash? slash slash!) "/foo")))

(deftest test-s3path
  (testing "s3/s3path"
    (is= "s3://bucket/key" (s3path "bucket" "key"))
    (is= "s3://bucket/key" (s3path "bucket" "/key"))
    (is= "s3://bucket/key/" (s3path "bucket" "key/"))
    (is= "s3://bucket" (s3path "bucket" ""))
    (is= "s3://bucket" (s3path "bucket" nil))
    (is (thrown? AssertionError (s3path nil "key")))))

(deftest test-s3path?
  (is (s3path? "s3://bkt/foo"))
  (is (s3path? "s3://bkt"))
  (is (not (s3path? "foo")))
  (is (not (s3path? "")))
  (is (not (s3path? nil)))
  (is (not (s3path? (File. "")))))

(deftest ^{:integration true} test-copy-object
  (testing "s3/copy-object"
    (with-bucket test-bucket  (fn []
      (let [key-a "unit/copy-a.txt"
            key-b "unit/copy-b.txt"]
        (put-object-string test-bucket key-a "more string test data")
        (is (not (object? test-bucket key-b)))
        (copy-object test-bucket key-a test-bucket key-b)
        (is (object? test-bucket key-b)))))))

(deftest ^{:integration true} test-put-get-objects
  (with-bucket test-bucket (fn []
    (let [src-path "resources/log4j.properties"
          dst-key-a "unit/object-a.txt"
          dst-key-b "unit/object-b.txt"
          test-data "string test data"
          tmp-file (File/createTempFile "s3-unit" nil)]
      (testing "s3/put-object"
        ;; put-object-string
        (is (not (object? test-bucket dst-key-a)))
        (put-object-string test-bucket dst-key-a test-data)
        (is (object? test-bucket dst-key-a))
        ;; put-object-data
        (is (not (object? test-bucket dst-key-b)))
        (put-object-data test-bucket dst-key-b src-path)
        (is (object? test-bucket dst-key-b)))
      (testing "s3/get-object-data"
        (is= test-data
             (String. (get-object-data test-bucket dst-key-a) "UTF-8"))
        ; download to a file
        (is (get-object-data test-bucket dst-key-a tmp-file))
        (is= (count test-data) (.length tmp-file))
        ; download existing file w/ an etag (i.e. download should be skipped)
        ; pass in a nil for s3, as further proof that the s3 service is not
        ; called upon to take any action.
        (is (false? (get-object-data test-bucket dst-key-a tmp-file
                        (.getETag (object? test-bucket dst-key-a)))))
        (is= (count test-data) (.length tmp-file))
        (is (nil? (get-object-data test-bucket "no-such-key"))))
      (testing "s3/objects"
        ; recursive
        (let [[obj-sum-1 obj-sum-2] (second (objects test-bucket nil nil))]
          (is= "unit/object-a.txt" (.getKey obj-sum-1))
          (is= "unit/object-b.txt" (.getKey obj-sum-2)))
        ; default delimiter
        (let [common-prefixes (first (objects test-bucket))]
          (is= "unit/" (first common-prefixes))))))))

(deftest test-parse-s3path
  (is= ["a.b" "c/d.txt"] (parse-s3path "s3://a.b/c/d.txt"))
  (is= ["a.b" ""] (parse-s3path "s3://a.b/"))
  (is= ["a.b" ""] (parse-s3path "s3://a.b"))
  (is= ["" ""] (parse-s3path "s3://"))
  (is= [nil nil] (parse-s3path "s3:/"))
  (is= nil (parse-s3path nil)))

(deftest test-new-s3-dest
  (let [new-s3-dest (ns-resolve 'com.climate.services.aws.s3 'new-s3-dest)
        sample-full-key "d1/d2/b"
        tmp (File. "/tmp")
        tmp-nd (File. "/tmp/new-dir")]
    (is= (File. "/tmp/d2/b")         (new-s3-dest tmp    "d1/d2"  sample-full-key))
    (is= (File. "/tmp/b")            (new-s3-dest tmp    "d1/d2/" sample-full-key))
    (is= (File. "/tmp/d2/b")         (new-s3-dest tmp    "d1/"    sample-full-key))
    (is= (File. "/tmp/d1/d2/b")      (new-s3-dest tmp    "d1"     sample-full-key))
    (is= (File. "/tmp/new-dir/d2/b") (new-s3-dest tmp-nd "d1/"    sample-full-key))
    (is= (File. "/tmp/new-dir/b")    (new-s3-dest tmp-nd "d1/d2/" sample-full-key))))

(deftest ^{:integration true} test-cp
  (with-bucket test-bucket
    (fn []
      (let [dir (File. "src/test/clj/resources")
            s3p (partial s3path test-bucket)
            tmp-dir (Files/createTempDir)
            tmp-dir-src (.getPath tmp-dir)]
        (testing "s3 upload"
          ; directory upload
          (cp dir (s3p "test-upload-dir"))
          (is (object? test-bucket "test-upload-dir/sample.csv"))
          (is (object? test-bucket "test-upload-dir/dates-file.txt"))
          (cp dir (s3path test-bucket "test-upload-dir/"))
          (is (object? test-bucket "test-upload-dir/resources/sample.csv"))
          ; upload dir (str with no slash at the end) to dir
          (cp "src/test/clj/resources" (s3p "foo2/"))
          (is (object? test-bucket "foo2/resources/sample.csv"))
          ; upload dir (str with slash at the end) to dir
          (cp "src/test/clj/resources/" (s3p "foo3/"))
          (is (object? test-bucket "foo3/sample.csv"))
          ; upload file to file
          (cp "src/test/clj/resources/dates-file.txt" (s3p "foo/bar.txt"))
          (is (object? test-bucket "foo/bar.txt"))
          ; upload file to dir
          (cp "src/test/clj/resources/dates-file.txt" (s3p "foo/baz/"))
          (is (object? test-bucket "foo/baz/dates-file.txt")))
        (testing "s3 download"
          (cp (s3p "foo/bar.txt") tmp-dir)
          (is (.exists (File. tmp-dir "bar.txt")))
          (cp (s3p "foo/bar.txt") (File. tmp-dir "bar2.txt"))
          (is (.exists (File. tmp-dir "bar2.txt")))
          (cp (s3p "foo/bar.txt") (str tmp-dir-src "/bar3.txt"))
          (is (.exists (File. tmp-dir "bar3.txt")))
          (cp (s3p "foo") (str tmp-dir-src "/new-dir"))
          (is (.exists (File. tmp-dir "new-dir/foo/bar.txt")))
          (is (.exists (File. tmp-dir "new-dir/foo/baz/dates-file.txt")))
          (cp (s3p "foo/") (str tmp-dir-src "/new-dir2"))
          (is (.exists (File. tmp-dir "new-dir2/bar.txt")))
          (is (.exists (File. tmp-dir "new-dir2/baz/dates-file.txt")))
          ; download existing, to test "sync" capability
          (.delete (File. tmp-dir "new-dir2/bar.txt"))
          (cp (s3p "foo/") (str tmp-dir-src "/new-dir2"))
          (is (.exists (File. tmp-dir "new-dir2/bar.txt"))))
        (testing "s3 copy within s3"
          (cp (s3p "foo/bar.txt") (s3p "foo/newdir/"))
          (is (object? test-bucket "foo/newdir/bar.txt"))
          (cp (s3p "foo/bar.txt") (s3p "foo/newdir/bar4.txt"))
          (is (object? test-bucket "foo/newdir/bar4.txt"))
          (cp (s3p "foo/bar.txt") (s3p "/"))
          ; same dest as prev, should overwrite, no error:
          (cp (s3p "foo/bar.txt") (s3p ""))
          (is (object? test-bucket "bar.txt")))))))
