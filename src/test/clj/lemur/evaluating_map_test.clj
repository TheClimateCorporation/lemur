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

(ns lemur.evaluating-map-test
  (:use
    lemur.evaluating-map
    clojure.test
    lemur.test
    midje.sweet)
  (:require
    [clojure.tools.logging :as log])
  (:import java.util.Date))

(deftest test-str-interpolate
  (let [vmap {:a "one" :b 2 :c "three" :bool? false}
        home (System/getenv "HOME")]
    (is= "a2c" (str-interpolate "a${b}c" vmap))
    (is= "onex2xthree" (str-interpolate "${a}x${b}x${c}" vmap))
    (is= "one" (str-interpolate "${a}" vmap))
    (is= "no vars" (str-interpolate "no vars" vmap))
    (is= "empty" (str-interpolate "empty${}" vmap))
    (is= (str "x" home "x") (str-interpolate "x${ENV.HOME}x" vmap))
    (is= "missing" (str-interpolate "missing${nokey}" vmap))
    (is= "$ {foo}" (str-interpolate "$ {foo}" vmap))
    (is= "xfalsex" (str-interpolate "x${bool?}x" vmap))
    (is= "" (str-interpolate "" vmap))))

(deftest test-evaluating-map
  (let [now (Date.)
        m {:runtime now
           :a "foo"
           :b #(+ 1 1)
           :c "b is ${b}, a is ${a}"
           :a-coll ["b is ${b}" "a is ${a}" (list 1 (constantly 1))]
           :a-map {:kw "b again ${b}" "${a}" [1 2]}
           :f1 (fn [emap] (:b emap))
           :f4 (fn [emap] "${b}")
           :remaining ["a1" "b2" "c3"]}
        emap (evaluating-map m)
        unused-steps [{:step-name "step-1" :a 1 :not-in-map-but-in-step "qux"}]
        emap-with-unused-steps (evaluating-map m unused-steps)]
    (is= 2 (emap :b))
    (is= 2 (get emap :b))
    (is= 2 (.get emap :b))
    (is= 2 (:b emap))
    (is= "b is 2, a is foo" (:c emap))
    (is= ["b is 2" "a is foo" [1 1]] (:a-coll emap))
    (is (not (empty? emap)))
    (is= 9 (count emap))
    (is (contains? emap :b))
    (is= {:a "foo" :b 2} (select-keys emap [:a :b]))
    (is= (set [:runtime :a :b :c :a-coll :a-map :f1 :f4 :remaining]) (set (keys emap)))
    (is= 2 (:f1 emap))
    (is= "2" (:f4 emap))
    ; key not found
    (is= nil (:not-in-map emap))
    ; unused step functionality
    (fact (:not-in-map-but-in-step emap-with-unused-steps) => anything
          (provided
            ; Providing log* instead of log/warn, b/c the latter is a macro
            (log/log* (as-checker identity) :warn nil #":not-in-map-but-in-step.+step-1") => nil :times 1))
    ; test with a Map as a value (keys and values should be evaluated).
    (is= {:kw "b again 2" "foo" [1 2]} (:a-map emap))))

(deftest test-re-find-map-entries
  (is= [[:foo.bbb 1] [:foo.aaa 2] [:foo.ccc 3]]
    (re-find-map-entries #"^foo\."
      {:foo.aaa 2 :foo.bbb 1 :notfoo -1 :foo.ccc 3 :foo.ddd nil}
      second)))
