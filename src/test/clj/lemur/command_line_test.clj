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

(ns lemur.command-line-test
  (:use
    lemur.command-line
    clojure.test
    lemur.common
    midje.sweet)
  (:require [com.climate.yaml :as yaml]))

(defn- ^{:default-doc "f1 dd"} f1 [] "f1")

(deftest test-pddd+formatted-help
  (fact
    (yaml/parse
         (formatted-help
           [[:foo "foo help" "1"]
            [:a-bool? "bool help"]
            [:bar "bar help"]
            [:baz "baz help" (with-meta (fn [] "baz") {:default-doc "baz-doc"})]
            [:pddd-fn "pddd fn help" (pddd f1)]]))
    => {:OPTIONS '(("foo" "foo help" "1")
                   ("a-bool?" "bool help" false)
                   ("bar" "bar help" nil)
                   ("baz" "baz help" "baz-doc")
                   ("pddd-fn" "pddd fn help" "f1 dd"))}))

(deftest test-parse-args
  (let [parse-args (ns-resolve 'lemur.command-line 'parse-args)]
    (fact
      (parse-args
        [[:foo "foo doc"] [:bar? "optional boolean"]]
        ["--foo" "my-foo" "--bar" "extra"])
        => [{:bar? false} {:foo "my-foo" :bar? true} ["extra"] nil]

      (parse-args
        [[:foo "foo doc"] [:bar? "optional boolean"]]
        ["--foo" "my-foo"])
        => [{:bar? false} {:foo "my-foo"} [] nil]

      (parse-args
        [[:foo "foo doc"] [:bar? "optional boolean" true]]
        ["--no-foo" "--no-bar"])
        => [{:bar? true} {:bar? false :foo nil} [] nil]

      ; this one also produces a WARN, although it's not tested for here.
      (parse-args
        [[:foo "foo doc"] [:bar? "optional boolean"]]
        ["--foo" "nil"])
        => [{:bar? false} {:foo "nil"} [] nil]

      (parse-args
        [[:foo "foo doc"] [:bar? "optional boolean"]]
        ["--foo"])
        => [{:bar? false} {} [] :foo]

      (parse-args
        [[:foo "foo doc" "default1"] [:bar? "optional boolean" true]]
        ["--baz" "baz-val"])
        => [{:bar? true :foo "default1"} {} ["--baz" "baz-val"] nil])))

(deftest test-validate
  (let [eopts
          {:keypair "tcc-key"
           :remaining []}
        validators
          [(val-opts :required :keypair)
           (val-remaining :empty true)]]
    (fact
      ; a successful test
      (validate [(mk-validator #(-> % :remaining empty?) "not empty")] eopts nil nil) => nil

      ; multiple successful tests
      (validate validators eopts nil nil) => nil

      ; multiple tests & a failed test
      (validate (conj validators (val-opts :numeric :keypair)) eopts nil nil)
        => (contains #":keypair.*numeric")

      ; test a std fn that returns false
      (validate [#(-> % :remaining map?)] eopts nil nil)
        => (contains #"Failed:.*fn")

      ; a failed test with non-nil exit-code triggers quit
      (validate validators {} nil 100) => anything
      (provided (quit :msg (contains #":keypair.*required") :cmdspec nil :exit-code 100) => anything :times 1))))

(deftest test-cl-handlers
  (let [context (atom {})]
    (add-command-spec* context [[:test1 "help"]])
    (add-command-spec* context [[:test2 "help"]])
    (fact (@context :command-spec) => [[:test1 "help"] [:test2 "help"]])))
