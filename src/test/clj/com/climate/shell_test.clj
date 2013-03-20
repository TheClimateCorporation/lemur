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

(ns com.climate.shell-test
  (:use
    com.climate.shell
    clojure.test
    lemur.test)
  (:require
    [clojure.java.io :as io])
  (:import
    [java.io File PipedInputStream PipedOutputStream]))

(deftest test-sh
  (is= "util test output\n" (:out (sh "echo" "util test" "output"))))

(deftest test-sh-with-stdout-capture
  (is= "util test output\n" (:out (sh "echo" "util test" "output" :err :pass))))

(deftest test-sh-with-pass
  (is= 0 (:exit (sh "echo" "util test" "2" :out :pass)))
  (is (pos? (:exit (sh "ls" "non-existant-file-hgf723gdn" :out :pass)))))

(deftest test-merge-env
  (is (re-find #"FOO=bar,HOME=.+"
        (:out (sh "bash" "-c" "echo FOO=$FOO,HOME=$HOME"
                  :err :pass
                  :env (merge-env {:FOO "bar"}))))))

(deftest test-merge-env-2
  (is (let [java-home (get (System/getenv) "JAVA_HOME")]
        (and
          (not-empty java-home)
          (.startsWith
            (:out (sh "java" "-cp" (clj-main-jar) "clojure.main" "-e"
                  "(vector
                     (get (System/getenv) \"JAVA_HOME\")
                     (get (System/getenv) \"FOO\"))"
                  :err :pass
                  :env (merge-env {:FOO "bar"})))
            (str "[" (pr-str java-home) " \"bar\"]"))))))

(deftest test-sh-with-files
  (let [txt "some test text"
        input (File/createTempFile "shell-unit" nil)
        output (File/createTempFile "shell-unit" nil)]
    (spit input txt)
    (is= 0 (:exit (sh "cat" :in input :out output :err :pass)))
    (is= txt (slurp output))))

(deftest test-sh-with-streams
  ; streams are managed external to (sh)
  (let [txt "some test text"
        input (File/createTempFile "shell-unit" nil)
        output (File/createTempFile "shell-unit" nil)]
    (spit input txt)
    (with-open [ins (io/input-stream input)
                outs (io/output-stream output)]
      (is= 0 (:exit (sh "cat" :in ins :out outs :err :pass))))
    (is= txt (slurp output))))

(deftest test-sh-with-fn
  ; this is one method to achieve a pipe
  (let [txt "b\na\n"
        input (File/createTempFile "shell-unit" nil)]
    (spit input txt)
    ; In function f below, we can't just return the stream that is passed in,
    ; b/c it will be closed right after it is given to us.
    (let [f #(sh "sort" :in % :err :pass)
          result (:out (sh "cat" :in input :out f :err :pass))]
      (is= 0 (:exit result))
      (is= "a\nb\n" (:out result)))))

; TODO constructing a pipe like this is still kind of tricky. Probably worth making some helper fn.
(deftest test-sh-piped
  ; This is similar to the sh expr `cat <input> | sort`
  (let [input (File/createTempFile "shell-unit" nil)]
    (spit input "b\na\n")
    (with-open [pipe-in (PipedInputStream.)]
      ; Note that we start the second process (sort) first (in a thread), so it is ready
      ; to accept data concurrently. And we block on the generating process, so we
      ; can close the stream it's writing to.  We need to close this stream first, or
      ; the 'sort' process will never finish.
      (let [result (future (sh "sort" :in pipe-in :err :pass))]
        (with-open [pipe-out (PipedOutputStream. pipe-in)]
          (sh "cat" :in input :out pipe-out :err :pass))
        (is= 0 (:exit @result))
        (is= "a\nb\n" (:out @result))))))
