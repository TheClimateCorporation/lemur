; Based on https://github.com/richhickey/clojure/blob/4bea7a529bb14b99d48758cfaf0d71af0997f0ff/src/clj/clojure/java/shell.clj
;
;   Copyright (c) Rich Hickey. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns com.climate.shell
  (:use
    [clojure.java.io :only [as-file copy input-stream output-stream]])
  (:import [java.io File InputStreamReader OutputStreamWriter]))

(def ^{:dynamic true} *sh-dir* nil)
(def ^{:dynamic true} *sh-env* nil)

(def ENV (System/getenv))

(defn clj-main-jar
  "Returns the jar in which clojure.main exists."
  []
  (-> (Class/forName "clojure.main")
      (.getClassLoader)
      (.getResource "clojure/main.class")
      (.getPath)
      (#(re-find #"file:(.*)!.*" %))
      (last)))

(defmacro with-sh-dir
  "Sets the directory for use with sh, see sh for details."
  {:added "1.2"}
  [dir & forms]
  `(binding [*sh-dir* ~dir]
     ~@forms))

(defmacro with-sh-env
  "Sets the environment for use with sh, see sh for details."
  {:added "1.2"}
  [env & forms]
  `(binding [*sh-env* ~env]
     ~@forms))

(defn- stream-seq
  "Takes an InputStream and returns a lazy seq of integers from the stream."
  [stream]
  (take-while #(>= % 0) (repeatedly #(.read stream))))

(defn- aconcat
  "Concatenates arrays of given type."
  [type & xs]
  (let [target (make-array type (apply + (map count xs)))]
    (loop [i 0 idx 0]
      (when-let [a (nth xs i nil)]
        (System/arraycopy a 0 target idx (count a))
        (recur (inc i) (+ idx (count a)))))
    target))

(defn- parse-args
  [args]
  (let [default-opts {:out "UTF-8" :dir *sh-dir* :env *sh-env*}
        [cmd opts] (split-with string? args)]
    [cmd (merge default-opts (apply hash-map opts))]))

(defn merge-env
  "Takes a map of environment settings and merges them with the SYSTEM
  ENVIRONMENT. Keys can be strings or keywords."
  [env]
  (merge {} ENV env))

(defn- as-env-string
  "Helper so that callers can pass a Clojure map for the :env to sh."
  [arg]
  (cond
   (nil? arg) nil
   (map? arg) (into-array String (map (fn [[k v]] (str (name k) "=" v)) arg))
   true arg))

(defn- handle-stream
  "Based on opt, handle the given stream.  See the :out option to (sh) for the
   meaning of opt. In the fn case, the fn should accept one arg, which will be
   the OutputStream. Run in a thread, and the return value is returned
   immediately in a future."
  [opt strm system-stream]
  (future
    (cond
      (= opt :bytes)
        (into-array Byte/TYPE (map byte (stream-seq strm)))
      (= opt :pass)
        (copy strm system-stream)
      (string? opt)
        (apply str (map char (stream-seq (InputStreamReader. strm opt))))
      (fn? opt)
        (opt strm)
      (instance? File opt)
        (with-open [os (output-stream opt)]
          (copy strm os))
      :default
        (copy strm opt))))

(defn sh
  "Passes the given strings to Runtime.exec() to launch a sub-process.

  Options are

  :in    may be given followed by a String, InputStream or File specifying text
         to be fed to the sub-process's stdin. Does not close any streams except
         those it opens itself (on a File).
  :out   may be given followed by :bytes, :pass, a File, a fn or a String.
         For...
         - String - it will be used as a character encoding name (for example
         \"UTF-8\" or \"ISO-8859-1\") to convert the sub-process's stdout to a
         String which is returned.
         - :bytes - the sub-process's stdout will be stored in a byte array and
         returned.
         - fn - it receives the stdout InputStream as an argument (the stream is
         closed automatically). The fn is run in a Thread, but sh blocks until
         the Thread completes. This can be used, for example, to filter the
         stream.
         - File - the output is written to the file.
         - :pass - the sub-process's stdout is passed to the main process stdout.
         Defaults to \"UTF-8\".
  :err   same options as :out. Defaults to the same value as :out.
  :env   override the process env with a map (or the underlying Java
         String[] if you are a masochist).
  :dir   override the process dir with a String or java.io.File.

  You can bind :env or :dir for multiple operations using with-sh-env
  and with-sh-dir.

  sh returns a map of
    :exit => sub-process's exit code
    :out  => sub-process's stdout (as byte[], String or InputStream)
    :err  => sub-process's stderr (as byte[], String or InputStream)
    "
  {:added "1.2"}
  [& args]
  (let [[cmd opts] (parse-args args)
        proc (.exec (Runtime/getRuntime)
                    (into-array cmd)
                    (as-env-string (:env opts))
                    (as-file (:dir opts)))
        input-future
          (future (cond
            (string? (:in opts))
              (with-open [osw (OutputStreamWriter. (.getOutputStream proc))]
                (.write osw (:in opts)))
            (nil? (:in opts))
              (.close (.getOutputStream proc))
            (instance? File (:in opts))
              (with-open [is (input-stream (:in opts))
                          os (.getOutputStream proc)]
                (copy is os))
            :default
              (with-open [os (.getOutputStream proc)]
                (copy (:in opts) os))))]
    (with-open [stdout (.getInputStream proc)
                stderr (.getErrorStream proc)]
      (let [out (handle-stream (:out opts) stdout System/out)
            err (handle-stream (get opts :err (:out opts)) stderr System/err)
            exit-code (.waitFor proc)]
        ;make sure input is done
        @input-future
        {:exit exit-code
         :out @out
         :err @err}))))

(comment

(println (sh "ls" "-l"))
(println (sh "ls" "-l" "/no-such-thing"))
(println (sh "sed" "s/[aeiou]/oo/g" :in "hello there\n"))
(println (sh "cat" :in "x\u25bax\n"))
(println (sh "echo" "x\u25bax"))
(println (sh "echo" "x\u25bax" :out "ISO-8859-1")) ; reads 4 single-byte chars
(println (sh "cat" "myimage.png" :out :bytes)) ; reads binary file into bytes[]
(println (sh "cat" :in (as-file "/tmp/input") :out (as-file "/tmp/out") :err :pass))

)