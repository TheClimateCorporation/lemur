;;; Original code copied from https://github.com/lancepantz/clj-yaml
;;; TODO contrib modifications back to original
;;; as of 2012-02-03

(ns com.climate.yaml
  (:use [clojure.java.io :only [reader]])
  (:import
    java.io.File
    [org.yaml.snakeyaml Yaml]))

(def ^{:dynamic true} *yaml* (Yaml.))

(defn- stringify [data]
  (cond
    (map? data)
      (into {} (for [[k v] data] [(stringify k) (stringify v)]))
    (coll? data)
      (map stringify data)
    (keyword? data)
      (name data)
    :else
      data))

(defn generate-string [data]
  (.dump *yaml* (stringify data)))

(defmulti to-seq class)

(defmethod to-seq java.util.LinkedHashMap [data]
  (into {} (for [[k v] data]
                 [(keyword k) (to-seq v)])))

(defmethod to-seq java.util.ArrayList [data]
  (map #(to-seq %) data))

(defmethod to-seq :default [data]
  data)

;TODO need to use .loadAll if the doc contains multiple documents
(defn parse [x]
  "x can be a String, a File, a Reader, or InputStream"
  (let [x (if (instance? File x) (reader x) x)]
    (to-seq (.load *yaml* x))))

(def parse-string parse)
