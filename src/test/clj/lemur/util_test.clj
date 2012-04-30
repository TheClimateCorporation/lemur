(ns lemur.util-test
  (:use
    lemur.util
    clojure.test
    lemur.test
    midje.sweet)
  (:require
    [midje.sweet :as midje]
    [clojure.java.io :as io]
    [com.climate.services.aws.s3 :as s3])
  (:import
    java.io.File
    com.google.common.io.Files))

(deftest test-lemur-merge
  ;bootstrap actions
  (let [m1 {:bootstrap-action.1 "a1" :b "b1"}
        m2 {:bootstrap-action.1 nil :c "c2"}
        m3 {:bootstrap-action.1 "a3" :b "b3"}]
    (is= {:bootstrap-action.1 nil :b "b1" :c "c2"} (lemur-merge m1 m2))
    (is= m1 (lemur-merge m1 nil))
    (is= {:bootstrap-action.1 "a3" :b "b3" :c "c2"} (lemur-merge m2 m3))
    (is= {:bootstrap-action.1 "a3" :b "b3" :c "c2"} (lemur-merge m1 m2 m3))
    (is= {:a 1 :c {:a 20 :d 10 :e 30}}
         (lemur-merge
           {:a 1 :c {:a 10 :d 10}}
           {:a 1 :c {:a 20 :e 30}}))
    (is (thrown? IllegalStateException (lemur-merge m1 m3))))
  ; :display-in-metajob
  (let [u1 {:foo 1 :display-in-metajob :foo}
        u2 {:bar 2 :display-in-metajob [:bar]}]
    (is= {:foo 1 :bar 2 :display-in-metajob [:foo :bar]} (lemur-merge u1 u2))
    (is= {:foo 1 :display-in-metajob :foo} (lemur-merge u1 {})))
  ; :upload
  (let [u1 {:foo 1 :upload ["u1"]}
        u2 {:foo 2 :upload "u2"}
        u3 {:foo 3 :upload ["u3"]}
        u4 {:foo 4 :upload (fn [] "u4")}]
    (is= {:foo 2 :upload ["u1" "u2"]} (lemur-merge u1 u2))
    (is= {:foo 3 :upload ["u1" "u3"]} (lemur-merge u1 u3))
    (is= "u4" ((:upload (lemur-merge u1 u4))))
    (let [result (lemur-merge u4 u1)]
      (is= 1 (:foo result))
      (is= "u4" ((-> result :upload first)))
      (is= "u1" (-> result :upload second))))
  ; remaining
  (let [r1 {:foo 1 :remaining 1}
        r2 {:foo 2 :bar 2}
        r3 {:foo 3 :remaining 3}]
    (is= {:foo 2 :bar 2 :remaining 1} (lemur-merge r1 r2))
    (is (thrown? IllegalStateException (lemur-merge r1 r3)))))

(deftest test-as-int
  (is= 0 (as-int nil))
  (is= 1 (as-int "1"))
  (is= 1 (as-int 1))
  (is= 1 (as-int 1.0))
  (is= 1 (as-int (Integer. 1))))

(deftest test-uuid
  (is (re-find #"^[-a-z0-9]{36}$" (uuid)))
  (is (re-find #"^[-a-z0-9]{8}$" (uuid 8))))

(deftest test-shortname
  (is= "foo" (shortname "/wb/clj/wb-clj/scripts/launch/foo-jobdef.clj"))
  (is= "foo" (shortname "/wb/clj/wb-clj/scripts/launch/foo_jobdef.clj"))
  (is= "foo" (shortname "/wb/clj/wb-clj/scripts/launch/foo.clj"))
  (is= "foo" (shortname "foo.clj"))
  (is= "foo" (shortname "/wb/clj/wb-clj/scripts/launch/foo")))

(deftest test-cp
  ; cp is a defmethod in util, for a multimethod defined in s3,
  ; the implementation is in com.climate.io, so we just test one simple
  ; case here, to make sure the multimethod is hooked-up
  (let [tmp-dir (Files/createTempDir)]
    ; one file, args: File, str
    (cp (io/file "src/test/resources" "sample.csv") tmp-dir)
    (is (.exists (io/file tmp-dir "sample.csv")))))

(deftest test-mk-absolute-path
  (let [cwd (System/getProperty "user.dir")]
    ; nil entries
    (is= "/tmp/foo" (mk-absolute-path nil "/tmp/foo"))
    (is= (str cwd "/foo") (mk-absolute-path nil "foo"))
    (is= "/tmp/foo" (mk-absolute-path "/tmp/foo" nil))
    (is= "s3://b/foo" (mk-absolute-path nil "s3://b/foo"))
    (is= "s3://b/foo/" (mk-absolute-path "s3://b/foo/" nil))
    (is= "s3://b/foo/" (mk-absolute-path "s3://b/foo" nil))
    ; string args, local, x is nil, and working-dir does not end with slash
    (is= "/tmp/foo" (mk-absolute-path "/tmp/foo" nil))
    ; string args, local
    (is= "/tmp/foo/c" (mk-absolute-path "/tmp/foo/" "c"))
    (is= "/tmp/foo/c" (mk-absolute-path "/tmp/foo" "c"))
    (is= "/tmp/foo/c/d" (mk-absolute-path "/tmp/foo" "c/d"))
    (is= "/c/d" (mk-absolute-path "/tmp/foo" "/c/d"))
    ; string args, s3
    (is= "s3://b/foo/c" (mk-absolute-path "s3://b/foo/" "c"))
    (is= "s3://b/foo/c/d" (mk-absolute-path "s3://b/foo" "c/d"))
    (is= "s3://b2/c" (mk-absolute-path "s3://b/foo" "s3://b2/c"))
    ; File args
    (is= "/tmp/foo/c/d" (mk-absolute-path (io/file "/tmp/foo") "c/d"))
    (is= "/tmp/foo/c/d" (mk-absolute-path "/tmp/foo" (io/file "c/d")))
    (is= "/c/d" (mk-absolute-path (io/file "/tmp/foo") (io/file "/c/d")))))

(deftest test-upload
  (testing "uploads from local to local."
    (let [dir (File. "src/test/resources")
          tmp-dir (Files/createTempDir)
          is-in-tmp-dir (fn [rel-path] (is (file-exists? (str tmp-dir "/" rel-path))))]
      (upload [["src/test/resources/sample.csv"]] tmp-dir)
      (is-in-tmp-dir "sample.csv")
      (upload [["src/test/resources/sample.csv" "a.csv"]] tmp-dir)
      (is-in-tmp-dir "a.csv")
      (upload [["src/test/resources/sample.csv" "d1/a.csv"]
               ["src/test/resources/sample.csv" "d1/"]]
              tmp-dir)
      (is-in-tmp-dir "d1/a.csv")
      (is-in-tmp-dir "d1/sample.csv")
      (upload [["src/test/resources/sample.csv" (str tmp-dir "/b.csv")]])
      (is-in-tmp-dir "b.csv"))))

(deftest ^{:integration true} test-s3-upload-and-file-exists?
  (let [file1 (File/createTempFile "utiltest-" nil)]
    (is (file-exists? (.getPath file1)))
    (is (not (file-exists? "/tmp/nonexistant-path")))
    ; s3 tests
    (let [bkt "lemur.unit2"]
      (with-bucket bkt (fn []
        (upload [["src/test/resources/sample.csv"]] (str "s3://" bkt "/dest/"))
        (is (file-exists? (s3/s3path bkt "dest/sample.csv")))
        (is (not (file-exists? (s3/s3path bkt "dest/non-existing")))))))))

(deftest test-dir-exists
  (let [tmp-dir (Files/createTempDir)]
    (fact
      (dir-exists? (.getPath tmp-dir)) => truthy
      (dir-exists? "tmp/not-a-dir-asdkhja") => falsey)))

(deftest test-data-path
  (is= "${data-uri}/foo" (data-path "/absolute/foo"))
  (is= "${data-uri}/foo" (data-path "s3://bkt/my/foo"))
  (is= "${data-uri}/foo" (data-path "foo"))
  (is= "${data-uri}/foo" (data-path "bar/foo")))

(deftest test-has-arity
  (midje/fact
    (has-arity? filter 2) => midje/truthy
    (has-arity? filter 1) => midje/falsey
    (has-arity? meta 1) => midje/truthy
    (has-arity? str 0) => midje/truthy))

(deftest ^{:integration true} test-fixed-retry-until
  (let [start-time (System/currentTimeMillis)
        result (fixed-retry-until (fn [] (vector 1)) 5 1 empty?)
        end-time (System/currentTimeMillis)]
    (is (nil? result))
    ; should take about 5000 ms, we wait 4500 to allow some buffer
    (is (> (- end-time start-time) 4500))))

(deftest test-collectify
  (is= [] (collectify nil))
  (is= ["foo"] (collectify "foo"))
  (is= ["foo"] (collectify ["foo"]))
  (is= ["foo" "bar"] (collectify ["foo" "bar"]))
  (is= [{:foo 1}] (collectify {:foo 1}))
  (is= [1 2] (collectify [1 2]))
  (is= [1] (collectify 1))
  (is= '(1) (collectify '(1)))
  (is= '(1) (collectify (seq '(1)))))

