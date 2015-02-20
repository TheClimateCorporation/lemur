(ns com.climate.services.aws.ec2
  (:require
    clojure.stacktrace)
  (:import
    [com.amazonaws.services.ec2 AmazonEC2Client]
    [com.amazonaws.services.ec2.model
     DescribeSpotPriceHistoryRequest
     DescribeInstancesRequest
     Filter]
    [com.amazonaws.auth AWSCredentialsProvider]
    [java.util Date GregorianCalendar]))

(def ^:dynamic *ec2* nil)

(defn ec2 [^AWSCredentialsProvider credentials]
  (AmazonEC2Client. credentials))

(def ec2-instance-details
  {"cc1.4xlarge"
   {:cpu "33.5",
    :family "cluster",
    :arch "64",
    :us-east-demand "1.3",
    :us-east-reserve "0.45",
    :mem "23.0",
    :type "cc1.4xlarge",
    :io "very-high"},
   "c1.xlarge"
   {:cpu "20",
    :family "high-cpu",
    :arch "64",
    :us-east-demand "0.66",
    :us-east-reserve "0.24",
    :mem "7.0",
    :type "c1.xlarge",
    :io "high"},
   "c1.medium"
   {:cpu "5",
    :family "high-cpu",
    :arch "32",
    :us-east-demand "0.165",
    :us-east-reserve "0.06",
    :mem "1.7",
    :type "c1.medium",
    :io "moderate"},
   "m2.4xlarge"
   {:cpu "26",
    :family "high-mem",
    :arch "64",
    :us-east-demand "1.8",
    :us-east-reserve "0.532",
    :mem "68.4",
    :type "m2.4xlarge",
    :io "high"},
   "m2.2xlarge"
   {:cpu "13",
    :family "high-mem",
    :arch "64",
    :us-east-demand "0.90",
    :us-east-reserve "0.266",
    :mem "34.2",
    :type "m2.2xlarge",
    :io "high"},
   "m2.xlarge"
   {:cpu "6.5",
    :family "high-mem",
    :arch "64",
    :us-east-demand "0.45",
    :us-east-reserve "0.133",
    :mem "17.1",
    :type "m2.xlarge",
    :io "moderate"},
   "m1.xlarge"
   {:cpu "8",
    :family "standard",
    :arch "64",
    :us-east-demand "0.64",
    :us-east-reserve "0.192",
    :mem "15",
    :type "m1.xlarge",
    :io "high"},
   "m1.large"
   {:cpu "4",
    :family "standard",
    :arch "64",
    :us-east-demand "0.32",
    :us-east-reserve "0.096",
    :mem "7.5",
    :type "m1.large",
    :io "high"},
   "m1.small"
   {:cpu "1",
    :family "standard",
    :arch "32",
    :us-east-demand "0.08",
    :us-east-reserve "0.024",
    :mem "1.7",
    :type "m1.small",
    :io "moderate"}})

(defn- price
  ([market instance-type]
    (price market "us-east" instance-type))
  ([market region instance-type]
    (-> ec2-instance-details
        (get (name instance-type))
        ((keyword (str region "-" market)))
        (#(if % (Double. %))))))

(def reserve-price (partial price "reserve"))
(def demand-price (partial price "demand"))

(defn spot-price-history
  "Get the spot price history for the given instance-type."
  ([]
   (spot-price-history 1 nil))
  ([hours]
   (spot-price-history hours nil))
  ([hours type]
   (let [[start end] (let [volatile-cal (GregorianCalendar.)
                           end (.getTime volatile-cal)]
                       (.add volatile-cal GregorianCalendar/HOUR_OF_DAY (- hours))
                       [(.getTime volatile-cal) end])
         sort-by (partial sort-by #(.getTimestamp %))
         req (doto (DescribeSpotPriceHistoryRequest.)
               (.setStartTime start)
               (.setEndTime end)
               (.setProductDescriptions ["Linux/UNIX"]))
         result (->> req
                  (.describeSpotPriceHistory *ec2*)
                  .getSpotPriceHistory)]
     (sort-by
       (if type
         (filter #(= (name type) (.getInstanceType %)) result)
         result)))))

(defn instance-tag-value
  "Given an instance, get the value for the given tag."
  [i tag]
  (let [tagged? #(= (.getKey %) tag)
        t (->> i
            .getTags
            (filter tagged?)
            first)]
    (if t (.getValue t))))

(defn instances-for-filter
  "filter the instances with the given filter-name, filter-values as defined for
  com.amazonaws.services.ec2.model.Filter."
  [filter-name & filter-values]
  (let [req (doto (DescribeInstancesRequest.)
              (.setFilters [(Filter. filter-name filter-values)]))]
    (-> *ec2* (.describeInstances req) .getReservations (->> (mapcat (memfn getInstances))))))

(defn instances-tagged
  "filter the instances for tag name, which must match one of the values."
  [name & values]
  (apply instances-for-filter (str "tag:" name) values))

(defn instance
  "Takes an instance-id and returns an Instance object."
  [instance-id]
  (when instance-id
    (->> (doto (DescribeInstancesRequest.) (.setInstanceIds [instance-id]))
      (.describeInstances *ec2*)
      .getReservations
      (mapcat (memfn getInstances))
      first)))
