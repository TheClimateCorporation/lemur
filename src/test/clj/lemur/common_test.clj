(ns lemur.common-test
  (:use
    lemur.common
    clojure.test
    midje.sweet)
  (:import
    java.io.File))

(defn- val-ok
  [val-result]
  (or (nil? val-result) (true? val-result) (empty? val-result)))

(deftest test-val-opts
  (let [eopts {:keypair "tcc-key"
               :int "100"
               :primitive-int 100
               :float "0.5"
               :negative "-1.0"
               :a-file "src/test/clj/resources/sample.csv"
               :a-dir "src/test/clj/resources"
               :non-word "a b"}]
    (fact "valid tests"
      ((val-opts :required [:keypair]) eopts) => val-ok
      ((val-opts :file :required [:a-file]) eopts) => val-ok
      ((val-opts :file [:a-file :foo]) eopts) => val-ok
      ((val-opts :local-dir [:a-dir]) eopts) => val-ok
      ((val-opts :required :word [:keypair]) eopts) => val-ok
      ((val-opts :required :numeric [:primitive-int]) eopts) => val-ok
      ((val-opts :numeric [:int :float :negative :foo]) eopts) => val-ok)
    (fact "negative tests"
      ((val-opts :required [:foo :bar]) eopts)
        => (contains #":foo =.*required" #":bar =.*required")
      ((val-opts :file [:keypair]) eopts)
        => (contains #":keypair = tcc-key.*file")
      ((val-opts :local-dir [:a-file]) eopts)
        => (contains #":a-file =.*not.*dir")
      ((val-opts :required :word [:non-word :foo]) eopts)
        => (contains #":non-word = a b.*only word" #":foo =.*required")
      ((val-opts :numeric [:keypair]) eopts)
        => (contains #":keypair = tcc-key.*numeric")
      ; with custom help message
      ((val-opts :numeric [:keypair] "custom help") eopts)
        => (contains "[:keypair = tcc-key] custom help"))))

(deftest test-val-remaining
  (let [mini-spec [:foo :bar :baz?]
        remaining {:remaining ["--foo" "a" "--baz" "--quux" "extra"]}
        remaining2 {:remaining ["--foo" "a" "--baz"]}]
    (fact "valid tests"
      ((val-remaining :min 2 :max 5) remaining) => val-ok
      ((val-remaining :min 5 "unused custom help") remaining) => val-ok
      ((val-remaining :mini-spec mini-spec :max 2) remaining) => val-ok
      ((val-remaining :mini-spec mini-spec :empty true) remaining2) => val-ok
      ((val-remaining :mini-spec mini-spec :required [:foo :baz?] :empty true) remaining2) => val-ok)
    (fact "negative tests"
      ((val-remaining :min 10 :max 1) remaining)
        => (contains #"Remaining.*at least 10" #"Remaining.*more than 1")
      ((val-remaining :mini-spec mini-spec :max 1) remaining)
        => (contains #"Remaining.*more than 1")
      ((val-remaining :mini-spec mini-spec :empty true "custom help") remaining)
        => (contains "Remaining [{:foo \"a\", :baz? true} [\"--quux\" \"extra\"]]: custom help")
      ((val-remaining :mini-spec mini-spec :required [:foo :bar :baz] :empty true) remaining2)
        => (contains #"Remaining.*required.*--bar"))))

(deftest test-aws-credentials-discovery
  (fact
    (aws-credentials-discovery) => #(and (instance? File %) (.exists %))))
