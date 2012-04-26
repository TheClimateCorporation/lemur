(ns com.climate.io
  (:require
    [clojure.java.io :as io]
    [clojure.string :as s]
    [clojure.tools.logging :as log])
  (:import
    java.io.File
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
