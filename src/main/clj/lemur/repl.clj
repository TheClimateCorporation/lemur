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

(ns lemur.repl
  "Initialize a repl environment.")

(println "Welcome to lemur repl.  (help) for details.")

(defn help
  []
  (println "
This repl namespace contains some repl tools, and some functions to quickly
load commonly used namespaces with standard aliases.

(api alias-or-namespace) - Print the public symbols of the namespace
(bootstrap-base)         - Common Clojure namespaces
(bootstrap-aws)          - s3, emr, and ec2 namespaces

Also remember the standard Clojure tools available in the repl. Try
  (dir clojure.repl)
  (dir clojure.stacktrace)
  (dir clojure.java.javadoc)
  "))

(defmacro api
  "Print the public symbols in a namespace. nspace can be either a fully qualified
  namespace name (not quoted), or a namespace alias.
  E.g. (api du)"
  [nspace]
  (let [nspace (the-ns (or (.lookupAlias *ns* nspace) nspace))]
    (println nspace)
    `(doseq [x# (->> (ns-publics ~nspace) (keys) (sort))]
       (println " " x#))))

(defmacro bootstrap-base []
  '(do
     ; debugging on the repl
     (use (quote clojure.stacktrace))

     ; common Clojure libraries
     (require (quote [clojure.java.io :as io]))
     (require (quote [clojure.pprint :as pprint]))
     (require (quote [clojure.string :as s]))
     (require (quote [clojure.template :as tmpl]))

     ; common Lemur libraries
     (require (quote [com.climate.io :as ccio]))
     (require (quote [com.climate.shell :as sh]))

     (ns-aliases *ns*)))

(defmacro bootstrap-aws []
  '(do
     (require (quote [com.climate.services.aws.s3 :as s3]))
     (require (quote [com.climate.services.aws.ec2 :as ec2]))
     (require (quote [com.climate.services.aws.emr :as emr]))
     (ns-aliases *ns*)))

(bootstrap-base)
