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

(ns lemur.bootstrap-actions
  (:use
    [lemur.evaluating-map :only [re-find-map-entries]]
    [clojure.pprint :only [cl-format]])
  (:require
    [clojure.string :as s]
    [clojure.tools.logging :as log]
    [com.climate.services.aws
     [emr :as emr]])
  (:import
    com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig))

(defn- ba-script-path
  [eopts script-name]
  (if (re-find #"/" script-name)
    script-name
    (format "s3://%s/%s/%s" (:bucket eopts) (:std-scripts-prefix eopts) script-name)))

(defn mk-hadoop-config
  "Assemble the hadoop config arguments from all keys in the opts that start with
  hadoop-config."
  [eopts]
  (mapcat second (re-find-map-entries #"^hadoop-config\.(\w+)" eopts)))

(defn- mk-bootstrap-action
  "Creates a BootstrapActionConfig instance from the arguments. name is a descriptive string,
  path can be an s3:// path or just a filename, which will be looked for in the standard
  bucket and prefix. args is an optional collection of strings which are used as command line
  arguments for the bootstrap action."
  ([eopts name path args]
   (when (and args (not (coll? args)))
     (throw (IllegalArgumentException.
         (format "Arguments to bootstrap-action %s must be a collection (args=%s)" name args))))
   (emr/bootstrap name (ba-script-path eopts path) args))
  ([eopts name path]
   (mk-bootstrap-action eopts name path []))
  ([eopts path-or-bac]
   (if (instance? BootstrapActionConfig path-or-bac)
     path-or-bac
     (mk-bootstrap-action eopts "Custom Config" path-or-bac))))

(defn mk-bootstrap-actions
  "Extracts the :bootstrap-action* entries from eopts and converts them to
  BootstrapActionConfig objects."
  [eopts]
  (let [order-number
          (fn [ba-key-value]
            (->> ba-key-value first name (re-find #"^bootstrap-action\.(\d+)") second Integer.))]
    (->> (re-find-map-entries #"^bootstrap-action\.(\d+)" eopts order-number)
      (map (fn [[key val]] [key (if (coll? val) val (vector val))]))
      (map (fn [[k v]] [k (apply mk-bootstrap-action eopts v)])))))

(def hadoop-config-files
  {"-c" "/home/hadoop/conf/core-site.xml"
   "-h" "/home/hadoop/conf/hdfs-site.xml"
   "-m" "/home/hadoop/conf/mapred-site.xml"})

(defn hadoop-config-details
  [eopts]
  (let [convert-single-config
              #(->> %
                 (partition 2)
                 (group-by first)
                 ;(sort-by first)
                 (map (fn [[k vals]]
                        (vector (hadoop-config-files k) (map second vals))))
                 (into {}))]
    (->> eopts
      (re-find-map-entries #"^hadoop-config\.\w+")
      (map (fn [[k v]] (vector k (convert-single-config v))))
      (into {}))))

(defn bootstrap-actions-details
  [ba-vector]
  (map
    (fn [[key ba]] (vector key
                           (.getName ba)
                           (.. ba getScriptBootstrapAction getPath)
                           (.. ba getScriptBootstrapAction getArgs)))
    ba-vector))
