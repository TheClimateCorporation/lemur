(ns com.climate.io-test
  (:use
    clojure.test
    lemur.test
    com.climate.io)
  (:require
    [clojure.java.io :as io])
  (:import
    java.io.File
    com.google.common.io.Files))

(deftest test-cp
  (let [dir (File. "src/test/clj/resources")
        tmp-dir (Files/createTempDir)
        tmp-dir-src (.getPath tmp-dir)
        exists? (fn [f] (.exists (io/file f)))]
    ; one file, args: File, str
    (cp (File. dir "sample.csv") tmp-dir)
    (is (exists? (File. tmp-dir "sample.csv")))
    ; one file to non-existant subdir, args: str, str
    (cp (str tmp-dir "/sample.csv") (str tmp-dir "/d1/d2/"))
    ; one file to a fully specified dest, args: str, str
    (cp (str tmp-dir "/sample.csv") (str tmp-dir "/d1/" "a.csv"))
    ; dir to dir
    (cp (str tmp-dir "/d1") (str tmp-dir "/d3/"))
    (is (exists? (str tmp-dir "/d3/d1/a.csv")))
    (is (exists? (str tmp-dir "/d3/d1/d2/sample.csv")))))

(deftest test-delete-directory
  (let [tmp-dir (Files/createTempDir)
        tmp-dir-src (.getPath tmp-dir)
        a-file (File. tmp-dir "some-file.txt")]
    (.createNewFile a-file)
    (is (.exists a-file))
    (delete-directory tmp-dir-src)
    (is (not (.exists a-file)))))

(deftest test-basename
  (is= "foo" (basename "foo"))
  (is= "foo" (basename "bar/foo"))
  (is= "foo" (basename "s3://bkt/path/foo"))
  (is= "foo" (basename "/bar/foo"))
  (is= "foo" (basename "foo/"))
  (is= "" (basename "/"))
  (is= "" (basename ""))
  (is= nil (basename nil))
  (is= "foo" (basename (File. "tmp/foo"))))

(deftest test-dirname
  (is= "" (dirname "foo"))
  (is= "bar" (dirname "bar/foo"))
  (is= "s3://bkt/path" (dirname "s3://bkt/path/foo"))
  (is= "/bar" (dirname "/bar/foo"))
  (is= "foo" (dirname "foo/"))
  (is= "/" (dirname "/"))
  (is= "" (dirname ""))
  (is= nil (dirname nil))
  (is= "tmp" (basename (dirname (File. "tmp/foo")))))
