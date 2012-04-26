;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Minimal Sample
;;; This is a small example, sufficient for many jobs.
;;; See sample-jobdef.clj for a more complete example with docs on each option
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(use-base
  'lemur.base
  'your.org.base)

(catch-args
  :num-days "Number of days of history to run.")

(defcluster sample-cluster
  :metajob-hipchat-room "marctest"
  :comment "This is a the Minimal Sample."
  ;:keep-alive? true
  :num-instances 1
  :slave-instance-type "m1.xlarge"
  :master-instance-type "m1.large"
  :upload ["/tmp/junk/a/foo.txt" "/tmp/junk/a/bar.txt"])

(defstep sample-step
  :main-class "your.org.job.Main"

  ;A value for --num-days that is only relevant when running with the :test profile
  :test {:num-days 1}

  ; These are example args to be passed to your hadoop job (in the order listed
  ; below). See the ARGS section of sample-jobdef.clj for a complete explanation.
  :args.stations "./stationsfile"
  :args.days "${num-days}"
  :args.passthrough true
  :args.positional ["foo" "bar baz"]
  :args.data-uri true)

(fire! sample-cluster sample-step)

;  *** RESULT ***
;
;  Running
;    lemur local minimal-sample-jobdef.clj --num-days 8 --extra-opt-to-pass
;
;  would result in something like:
;     hadoop jar your.org.job.Main --stations ./stationsfile \
;     --days 8 \
;     --extra-opt-to-pass foo "bar baz" /tmp/minimal-sample-2012-01-27-8da30ff3/data
