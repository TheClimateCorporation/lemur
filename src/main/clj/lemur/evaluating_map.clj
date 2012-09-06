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

(ns lemur.evaluating-map
  (:require
    [lemur.util :as util]
    [clojure.string :as s]
    [clojure.tools.logging :as log])
  (:import
    [clojure.lang MapEntry RT]))

(def ENV (System/getenv))

(defn eoval
  [key]
  (with-meta
    (fn [eopts] (eopts key))
    {:eoval true :key key}))

(defn str-interpolate
  "Replace 'variables' in s with the correspondig map value from value-map. Variables
  are encoded into the string inside ${}, for example
    \"ab${letter}def\"
  with value-map
    { :letter \"c\" }
  would yield
    \"abcdef\"
  Note that the map-value keys should be keywords."
  [s value-map]
  (-> s
    (s/replace #"\$\{ENV\.([^\}]+)\}" #(->> % second (get ENV) str))
    (s/replace #"\$\{([^\}]*)\}" #(-> % second keyword value-map str))))

(defn evaluate
  "Get the value from the evaluating-map, if it is a function get the result of the
  function (which is evaluated recursively). For strings, interpolate variables in
  the string.  For collections, evaluate (recursively) each entry in the collection.
  (see evaluating-map)"
  [orig-map key emap unused-steps]
  (let [x (get orig-map key)]
    (cond
      ; When orig-map does not have the key, and unused-steps have been supplied...
      (and (not (contains? orig-map key)) unused-steps)
        (when-let [found-in-steps
                     (->> unused-steps
                       (filter #(->> key (get %) nil? not))
                       (map :step-name)
                       seq)]
          (log/warn (format (str "No value for %s. But this key does exist in step(s): %s. "
                                 "Maybe you need to define the key in defcluster instead?")
                            key found-in-steps)))
      (fn? x)
        ; If there are circular dependencies, it will eventually throw a StackOverflowError
        (let [result (cond
                       (:eoval (meta x)) (x emap)
                       (util/has-arity? x 0) (x)
                       (util/has-arity? x 1) (x emap))]
          (evaluate {key result} key emap unused-steps))
      (map? x)
        (into {} (map vec (map #(evaluate {key %} key emap unused-steps) x)))
      (coll? x)
        (map #(evaluate {key %} key emap unused-steps) x)
      (string? x)
        (str-interpolate x emap)
      :default
        x)))

(defn evaluating-map
  "Creates a READ-ONLY map from Map m, where a key lookup is evaluated. If it is a function,
  it is called with this evaluating-map as its arg. For example:

    (fn [eopts] (str (eopts :app-name) \"-\" (:runtime rt)))

  For strings, interpolate variables in the string. For collections, evaluate (recursively)
  each entry in the collection. In the function case, the result of the function is again
  evaluated, other types are returned as is.

  Also see core/lfn for a convenient helper macro for creating these functions.

  The optional argument unused-steps, is a coll of maps (presumably created by defstep). If
  the key can not be found in m, but is found in one of the maps in unused-steps, then
  output a warning. A common mistake is to define a value in defstep, when it needs to be in
  defcluster."
  ; There is some repetitive work in evaluating fns that reference other keys, but this
  ; is negligible in terms of performance, since the number of executions is so small.
  ([m]
   (evaluating-map m nil))
  ([m unused-steps]
   (reify clojure.lang.Associative
          clojure.lang.IPersistentMap
          java.util.Map
          clojure.lang.IFn
     (get [this key] (evaluate m key this unused-steps))
     (toString [this] (RT/printString this))
     (entryAt [this key] (MapEntry. key (evaluate m key this unused-steps)))
     (valAt [this key] (evaluate m key this unused-steps))
     (seq [this] (seq m))  ;known-bug: the values are not evaluated, but it is useful for the fn (keys)
     (containsKey [this key] (contains? m key))
     (count [this] (count m))
     (empty [this] (empty? m))
     (invoke [this key] (evaluate m key this unused-steps)))))

(defn re-find-map-entries
  "Return map entries from the given map, whose keys match pattern and values are not
  nil.  The entries will be sorted by keyfn if given, or just natural sort otherwise."
  ([pattern m keyfn]
   (->> m
     keys
     (filter (comp (partial re-find pattern) name))
     ; work on keys, rather than map-entries, so that this works for evaluating-maps
     (map #(vector % (get m %)))
     (sort-by keyfn)
     (filter #(-> % second identity))))
  ([pattern m]
    (re-find-map-entries pattern m identity)))
