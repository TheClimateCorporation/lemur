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

(ns lemur.core-test
  (:use
    lemur.core
    [lemur.command-line :only [quit]]
    [lemur.evaluating-map :only [evaluating-map]]
    midje.sweet
    clojure.test
    lemur.test)
  (:require
    [clojure.string :as s]
    [com.climate.yaml :as yaml]
    [com.climate.services.aws.emr :as emr])
  (:import
    java.util.Date))

(use-base
  'lemur.base)

; fixtures

(context-set :command "dry-run")

(defcluster lemon-cluster
  :bucket "lemon-bkt"
  :keypair "tcc-key"
  :scripts-src-path "ops/conf/hadoop/emr-bootstrap"
  :runtime-jar "s3://aaa/bbb/jars/a.jar"
  :some-profile {:main-class "lemur.some.profile.class"})

(defstep foo-step
  :main-class "lemur.some.classname"
  :x "3"
  :args.foofoo "1"
  :args.should-be-ignored nil
  :args.data-uri true
  :args.positional ["2" "${x}"])

(defstep bar-step
  :main-class "lemur.some.other.classname"
  :args.a-bool true
  :args.b-bool false
  :args.data-uri false
  :args.positional ["1"])

(let [orig-context @context
      reset-context
        (fn [f]
          (swap! context (constantly orig-context))
          (f))]
  (use-fixtures :each reset-context))

;tests

(deftest test-context-opts
  (is nil? (context-get :foo))
  (context-set :foo "bar")
  (is= "bar" (context-get :foo))
  (is nil? (context-get :baz)))

(deftest test-cli-opt
  (context-set :command-spec [])
  (catch-args :a nil :b nil)
  (context-set :raw-args ["--a" "1" "--b" "b"])
  (is= "1" (cli-opt :a))
  (is= "1" (cli-opt :a))
  (is= {:a "1" :b "b"} (cli-opt)))

(deftest test-catch-args
  (context-set :command-spec [])
  (catch-args
    :foo "foo help"
    :bar? "bar boolean"
    [:with-default "some opt" 1])
  (is= #{[:foo "foo help"]
         [:bar? "bar boolean"]
         [:with-default "some opt" 1]}
       (set (context-get :command-spec))))

(deftest test-full-options
  ; A coll arg to add-profiles, with keywords
  (add-profiles [:ec2 :another-one])
  (context-set :base {:foo "foo1" :orig "orig"})
  (fact (full-options {:bar "bar1"
                       :bool? false
                       :ec2 {:bar "bar2" :foo "foo2"}})
          => (contains {:command "dry-run"
                        :bar "bar2"
                        :foo "foo2"
                        :bool? false
                        :orig "orig"}))
  ; A single arg, as a string instead of a keyword
  (add-profiles ":prof1")
  (fact (full-options {:baz "oh no" :prof1 {:baz "ok"}})
          => (contains {:baz "ok"}))
  ; Merge of profile maps
  (context-set :base {:a 1 :b 2 :prof1 {:a 10 :d 10} :e {:a "wrong"}})
  (fact (full-options {:a 200 :b 2 :d 100 :prof1 {:a 20} :e 30})
          => (contains {:command "dry-run"
                        :a 20 :b 2 :d 10 :e 30}))
  ; :args.foo inside an active profile is an ERROR
  (fact
    (full-options {:a 1 :b 2 :prof1 {:args.foo 1}}) => anything
    (provided
      (quit :msg (as-checker string?) :exit-code 22) => anything :times 1)))

(deftest test-process-args-with-full-options
  (let [process-args-if-needed (ns-resolve 'lemur.core 'process-args-if-needed)]
    (context-set :base {:foo "0" :qux "0" :baz "0"})
    (context-set :command-spec [[:foo nil "1"] [:qux nil "1"] [:bar? nil] [:baz nil "1"]])
    (context-set :raw-args ["--foo" "3"])
    (process-args-if-needed)
    (fact (full-options {:foo "2" :qux "2"})
            => (contains {:command "dry-run"
                          :foo "3"     ; actual command line wins over defcluster
                          :bar? false  ; implicit command line default
                          :baz "1"     ; explicit command line default
                          :qux "2"}    ; defcluster wins over command-line default
                          ))))

(deftest test-parse-upload-property
  (let [parse-upload-property
          (ns-resolve 'lemur.core 'parse-upload-property)]
    (is= [["foo.txt" "a"] ["baz.txt"] ["bar.txt" "b"]]
         (parse-upload-property
           {:bar "bar.txt"
            :upload ["foo.txt" :to "a", "baz.txt", :bar :to "b"]}))))

(deftest test-defstep+mk-steps
  (let [mk-steps
          (ns-resolve 'lemur.core 'mk-steps)
        cluster
          (assoc lemon-cluster :keep-alive? true :run-id "RUNID")
        [result1 result2]
          (mk-steps cluster [foo-step bar-step])
        extract-values
          (juxt
            (memfn getName)
            (memfn getActionOnFailure)
            #(.. % getHadoopJarStep getJar)
            #(.. % getHadoopJarStep getMainClass)
            #(.. % getHadoopJarStep getArgs)
            #(.. % getHadoopJarStep getProperties))
        base-uri (:base-uri (evaluating-step cluster foo-step))
        runtime-jar-path "s3://aaa/bbb/jars/a.jar"]
    (is=
      ["foo" "CANCEL_AND_WAIT" runtime-jar-path "lemur.some.classname" ["--foofoo" "1" (str base-uri "/data") "2" "3"] []]
      (extract-values result1))
    (is=
      ["bar" "CANCEL_AND_WAIT" runtime-jar-path "lemur.some.other.classname" ["--a-bool" "1"] []]
      (extract-values result2))
    ; test boolean with false default is implicitly added to catch-args, and can be turned on
    (context-set :raw-args ["--b-bool"])
    (is=
      ["bar" "CANCEL_AND_WAIT" runtime-jar-path "lemur.some.other.classname" ["--a-bool" "--b-bool" "1"] []]
      (extract-values (first (mk-steps cluster [bar-step]))))
    (context-set :raw-args [])
    ; test that :args.foofoo is a fn of :foofoo
    (is=
      ["foo" "CANCEL_AND_WAIT" runtime-jar-path "lemur.some.classname" ["--foofoo" "2" (str base-uri "/data") "2" "3"] []]
      (extract-values (first (mk-steps (assoc cluster :foofoo 2) [foo-step]))))
    ; test with an arg set to nil on the command line  (:foofoo should be implicitly applied to catch-args via the defstep)
    (context-set :raw-args ["--no-foofoo"])
    (is=
      ["foo" "CANCEL_AND_WAIT" runtime-jar-path "lemur.some.classname" [(str base-uri "/data") "2" "3"] []]
      (extract-values (first (mk-steps cluster [foo-step]))))
    (context-set :raw-args [])
    ; extra arg in step, not in original defstep-- should trigger an error
    (fact
      (mk-steps (assoc cluster :args.extra 1) [foo-step])
        => anything
      (provided
        (quit :msg (as-checker string?) :exit-code 20) => anything :times 1))))

(deftest test-lfn
  (let [register (atom 0)
        inc-on-uniq (lfn [foo] (swap! register inc) foo)]
    (is= "a" (inc-on-uniq {:foo "a" :junk 2}))
    (is= "b" (inc-on-uniq {:foo "b"}))
    (is= "b" (inc-on-uniq {:foo "b"}))
    (is= 2 @register)))

(deftest test-with-eopts
  (is= "s3://aaa/bbb/jars/a.jar" (with-eopts [lemon-cluster] [eopts] (:runtime-jar eopts))))

(deftest test-steps-for-active-profile
  (let [steps-for-active-profiles
          (ns-resolve 'lemur.core 'steps-for-active-profiles)]
    (context-set :profiles [:none])
    (is= [{:step 1} {:step 2}]
      (steps-for-active-profiles [{:step 1} {:step 2} :prof1 {:step 3} :prof2 [{:step 4} {:step 5}]]))
    (context-set :profiles [:prof1 :other])
    (is= [{:step 1} {:step 2} {:step 3}]
      (steps-for-active-profiles [[{:step 1} {:step 2}] :prof1 {:step 3} :prof2 [{:step 4} {:step 5}]]))
    (context-set :profiles [:prof1 :prof2])
    (is= [{:step 1} {:step 2} {:step 3} {:step 4} {:step 5}]
      (steps-for-active-profiles [{:step 1} {:step 2} :prof1 {:step 3} :prof2 [{:step 4} {:step 5}]]))
    (context-set :profiles [])))

(deftest test-add-fns-to-coll
  ; test is done using add-hooks, but add-validators is equivalent
  (let [orig-hooks (context-get :hooks)]
    (against-background [(before :contents (clear-hooks))
                         (after :contents (context-set :hooks orig-hooks))]
       (add-hooks
         str
         true int
         (= 1 2) empty?)
       (fact (context-get :hooks) => [str int])
       (clear-hooks)
       (fact (context-get :hooks) => empty?))))

(deftest test-use-base+fire!-dry-run
  (context-set :profiles [:some-profile])
  (let [cluster (merge lemon-cluster
                       {:runtime "RT" :custom-value "a-string" :display-in-metajob :custom-value})
        eopts (evaluating-map (full-options cluster))
        bkt (:bucket eopts)
        run-id (:run-id eopts)]
      (let [metajob (-> (fire! cluster foo-step :some-profile [bar-step])
                      s/join
                      yaml/parse)
            jfo-key (keyword "Jobflow Options")]
        (fact
          (get-in metajob [:Environment :base-uri]) => #"^s3://lemon-bkt/runs/core_test/RT-[\da-f]{8}$"
          (get-in metajob [:Environment :app]) => "core_test"
          (get-in metajob [:Environment :custom-value]) => "a-string"
          (get-in metajob [jfo-key :emr-name]) => run-id
          (get-in metajob [jfo-key :keypair]) => "tcc-key"
          metajob => (contains {:Profiles [":some-profile"]
                                :Steps (n-of map? 2)}))))
  (context-set :profiles []))
