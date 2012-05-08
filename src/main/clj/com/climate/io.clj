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

(ns com.climate.io
  (:require
    [clojure.java.io :as io]
    [clojure.string :as s]
    [clojure.tools.logging :as log])
  (:import
    java.io.File
    java.util.Properties
    [org.apache.commons.io FileUtils]))

(defn cp
  "Copy files or directories (recursively). src and dest can be absolute or relative;
  and can be string paths, File objects or URLs."
  [src dest]
  (let [src (io/file src)
        destf (io/file dest)
        dest-dir? (or (.isDirectory destf)
                      (and (string? dest) (= (last dest) \/)))]
    (cond
      ; f   f    - copyFile
      (and (.isFile src) (not dest-dir?))
        (FileUtils/copyFile src destf)
      ; f   d/   - copyFileToDirectory
      (and (.isFile src) dest-dir?)
        (FileUtils/copyFileToDirectory src destf)
      ; d   d2   - copyDirectory
      (and (.isDirectory src) (not dest-dir?))
        (FileUtils/copyDirectory src destf)
      ; d   d2/  - copyDirectoryToDirectory
      (and (.isDirectory src) dest-dir?)
        (FileUtils/copyDirectoryToDirectory src destf))))

(defn delete-directory
  "Delete the directory and it's contents."
  [dir]
  (FileUtils/deleteDirectory (io/file dir)))

(defn dirname
  "Get directory name.  I.e. the path upto the final / (not including the /)."
  [x]
  (cond
    (nil? x) nil
    (instance? File x) (.. x getCanonicalFile getParent)
    (= x "/") "/"
    (re-find #"^.*/$" x) (subs x 0 (dec (count x)))
    :default (s/join "/" (butlast (s/split x #"/")))))

(defn basename
  "Get the final segment of the S3 path, key, or File. Using / as a delimiter."
  [x]
  (cond
    (nil? x) nil
    (instance? File x) (.. x getCanonicalFile getName)
    (= x "/") ""
    :default (last (s/split x #"/"))))

(defn load-properties
  "Read properties from read-able. Will coerce read-able into a reader
  using clojure.java.io/reader"
  ([read-able] (load-properties read-able (Properties.)))
  ([read-able default-properties]
     (with-open [f (io/reader read-able)]
       (doto (Properties. default-properties)
         (.load f)))))
