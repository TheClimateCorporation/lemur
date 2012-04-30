;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Generic Launcher
;;;
;;; For launching a simple job (one step), where the details are specified
;;; on the command line.
;;;
;;; Example of common usage:
;;; lemur dry-run generic-jobdef.clj --app "prod-348" --num-instances 3 \
;;;   --main-class weatherbill.hadoop.query.prod-348 --slave-instance-type m2.xlarge \
;;;   --bucket 'com.weatherbill.${env}.banzai'
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(catch-args
  :main-class "A fully qualified classname."
  :app "A representative name the app-- no spaces, as it is used in paths"
  :bucket "An s3 bucket, e.g. 'com.weatherbill.${env}.banzai'")

(defcluster generic-cluster
  :emr-name "${app}")

(defstep generic-step
  :args.passthrough true
  :args.data-uri true)

(fire! generic-cluster generic-step)
