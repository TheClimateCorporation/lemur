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

(ns lemur.bootstrap-actions-test
  (:use
    lemur.bootstrap-actions
    clojure.test
    lemur.test)
  (:require
    [com.climate.services.aws.emr :as emr]))

(deftest test-ba-script-path
  (let [ba-script-path (ns-resolve 'lemur.bootstrap-actions 'ba-script-path)
        fopts {:bucket "aaa" :std-scripts-prefix "bbb"}]
    (is= "s3://aaa/bbb/myscript.sh"
         (ba-script-path fopts "myscript.sh"))
    (is= "s3://foo/myscript.sh"
         (ba-script-path fopts "s3://foo/myscript.sh"))))

(deftest test-mk-hadoop-config
  (is= ["-m" "aaa=bbb" "-h" "ccc=ddd" "-m" "foo=bar" "-h" "boo=baz"]
       (mk-hadoop-config
         {:hadoop-config.b ["-m" "foo=bar" "-h" "boo=baz"]
          :hadoop-config.a ["-m" "aaa=bbb" "-h" "ccc=ddd"]})))

(defn- validate-bac
  "Compare the BootstrapActionConfig object to a vector of expected values."
  [expected bac]
  (is= expected
       ((juxt (memfn getName)
              #(.. % getScriptBootstrapAction getPath)
              #(.. % getScriptBootstrapAction getArgs)) bac)))

(deftest test-mk-bootstrap-action
  (let [mk-bootstrap-action (ns-resolve 'lemur.bootstrap-actions 'mk-bootstrap-action)
        eopts {:bucket "aaa" :std-scripts-prefix "bbb"}
        bac (emr/bootstrap "My BA" "s3://aaa/bbb/my-ba.sh" [])]
    (is= bac (mk-bootstrap-action nil bac))
    (validate-bac ["My BA" "s3://aaa/bbb/my-ba.sh" ["a"]]
         (mk-bootstrap-action eopts "My BA" "s3://aaa/bbb/my-ba.sh" ["a"]))
    (validate-bac ["My BA" "s3://aaa/bbb/my-ba.sh" []]
         (mk-bootstrap-action eopts "My BA" "s3://aaa/bbb/my-ba.sh"))
    (validate-bac ["Custom Config" "s3://aaa/bbb/my-ba.sh" []]
         (mk-bootstrap-action eopts "s3://aaa/bbb/my-ba.sh"))
    (validate-bac ["Custom Config" "s3://aaa/bbb/my-ba.sh" []]
         (mk-bootstrap-action eopts "my-ba.sh"))))

(deftest test-mk-bootstrap-actions
  (let [mk-bootstrap-actions
          (ns-resolve 'lemur.bootstrap-actions 'mk-bootstrap-actions)
        eopts
          {:bucket "aaa"
           :std-scripts-prefix "bbb"
           :bootstrap-action.100 "my-ba.sh"
           :bootstrap-action.200 ["Another One" "s3://foo/my-ba.sh" ["a"]]}
        ba-vector
          (mk-bootstrap-actions eopts)
        [ba1 ba2]
          (map second ba-vector)]
    (is= [:bootstrap-action.100 :bootstrap-action.200] (map first ba-vector))
    (validate-bac ["Custom Config" "s3://aaa/bbb/my-ba.sh" []] ba1)
    (validate-bac ["Another One" "s3://foo/my-ba.sh" ["a"]] ba2)))

