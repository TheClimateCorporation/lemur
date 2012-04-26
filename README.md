### Overview

lemur is a tool to launch hadoop jobs locally or on EMR based on a configuration
file, referred to as a jobdef. The general command line format is:

```
lemur <command> <jobdef-file> [options] [remaining]

lemur help                    - display this help text
lemur run ./jobdef.clj        - Run a job on EMR
lemur dry-run ./jobdef.clj    - Dry-run, i.e. just print out what would be done
lemur start ./jobdef.clj      - Start an EMR cluster, but don't run the steps (jobs)
lemur local ./jobdef.clj      - Run the job using local hadoop (e.g. standalone mode)
lemur display-types           - Display the instance-types with basic stats and exit
lemur spot-price-history      - Display the spot price history for the last day and exit
```
###### Examples
```
lemur run clj/wb-clj/scripts/launch/hrap-jobdef.clj --dataset ahps --num-days 10
lemur start clj/wb-clj/src/weatherbill/lemur/sample-jobdef.clj
```

### The Job Definition (jobdef file)

A jobdef file describes your EMR cluster, local environment, pre- and post-actions (aka hooks) and zero or more "steps".  A step is Amazon's name for a task or job submitted to the cluster.  Lemur reads your jobdef, which defines all your options inside (defcluster) and (defstep).  Finally, at the end of your jobdef, you execute (fire! ...) to make things happen.  Also keep in mind that the jobdef is an interpreted clj file, so you can insert arbitrary Clojure code to be executed anywhere in the file (but see HOOKS below for a better way).
