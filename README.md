### Overview

Lemur is a tool to launch hadoop jobs locally or on EMR, based on a configuration file, referred to as a jobdef. The jobdef file describes your EMR cluster, local environment, pre- and post-actions (aka hooks) and zero or more "steps".  A step is Amazon's name for a task or job submitted to the cluster.  Lemur reads your jobdef, at the end of your jobdef, you execute (fire! ...) to make things happen.  Also keep in mind that the jobdef is an interpreted clj file, so you can insert arbitrary Clojure code to be executed anywhere in the file (but see HOOKS below for a better way).

##### Features

- Launch EMR cluster and submit step(s); or run against local hadoop (usually hadoop standalone for dev and testing)
- Basic configuration options include:
-- Bootstrap actions
-- Hadoop config
-- Uploads (files to transfer to S3, or local)
-- Cluster details (num instances, master instance type, etc)
-- Output paths to use for data, logs, main jar, etc.
-- Support for spot market instances
- Profile support provides packages of options and functionality that can be enabled or disabled (e.g. you can have a :test profile or a :live profile)
- Validation for your command line options and environment before launching EMR and your job
- Override configured options via command line
- Hooks for actions that should be triggered before or after job launch (e.g. one hook in use at Climate Corporation does a diff on the results of a local run, as a full integration test.  Another hook, posts a detailed message to IRC-- hipchat-- when a new job is started)
- Optionally wait for an EMR job to complete
- A dry-run feature, so you can check the final cluster configuration, arguments that will be sent to your hadoop main, etc.
- All the details from dry-run (cluster/step config, etc) are persisted with each job run
- All settings can be literal values, interpolated strings (e.g. set the S3 bucket as "com.your-co.${env}.hadoop"), or functions for ultimate flexibility
- Import common options, functionality and behavior to avoid duplication (i.e. DRY principle)
- Pass-through command-line options, allows you to specify extra args on the command line that are meaningful to your hadoop main function, but are unknown to lemur or your jobdef

### Installation

1. Download the tar-gzip from the GitHub Downloads link
1. Expand into some install location
1. set LEMUR_HOME to the top of the install path
1. set LEMUR_EXTRA_CLASSPATH to any classpath entries (colon separated) that you want lemur to include when it runs your jobdef. The classpath that includes you base files, or other functions or libraries for use by your jobdefs for example.

### Compatibility

Clojure 1.2 or Clojure 1.3 (although I haven't done extensive testing with 1.3 yet).

I've used lemur on Mac OS X and Linux.  It should work on Windows, but you will need to make your own version of the Bash script bin/lemur (or use cygwin).  I would be interested to know if this works on Windows, please let me know if you try it (patches welcome).

### Usage

The general command line format is:

```
bin/lemur <command> <jobdef-file> [options] [remaining]

bin/lemur help                    - display this help text
bin/lemur run ./jobdef.clj        - Run a job on EMR
bin/lemur dry-run ./jobdef.clj    - Dry-run, i.e. just print out what would be done
bin/lemur start ./jobdef.clj      - Start an EMR cluster, but don't run the steps (jobs)
bin/lemur local ./jobdef.clj      - Run the job using local hadoop (e.g. standalone mode)
```
###### Examples
```
lemur run clj/wb-clj/scripts/launch/hrap-jobdef.clj --dataset ahps --num-days 10
lemur start clj/wb-clj/src/weatherbill/lemur/sample-jobdef.clj
```

### Help

- Execute 'bin/lemur help' for more details on the concepts
- Look at examples/sample-jobdef.clj for details on all options that you can use in your jobdef
- You can ask questions on the Google Group

Feedback and feature requests are welcome!
