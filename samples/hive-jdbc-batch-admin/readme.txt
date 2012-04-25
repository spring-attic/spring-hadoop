======================
== Hive JDBC Demo   ==
======================

1. MOTIVATION

The demo is on how to use Hive with Spring Hadoop. The demo
requires a running Hadoop instance (by default at localhost:9000) and Hive instance
The Hadoop settings can be configured through hadoop.properties (more info in the Spring Hadoop reference docs).



2. BUILD AND DEPLOYMENT

This directory contains the source files.
For building, JDK 1.6+ are required

To build and run the sample with Spring Batch Admin:

ensure Hive server is running.

$ ../gradlew downloadSampleSet

$ ../gradlew jettyRun

then open internet explorer and input "http://localhost:8081/hive-jdbc-batch-admin", click "Jobs" -> "hiveJob". In the Job Parameters text field,
 input "fail=false run.id=1", and click "Launch" button.

3. IDE IMPORT

To import the code inside an IDE run the command

For Eclipse 
$ ../gradlew eclipse

For IDEA
$ ../gradlew idea

This will generate the IDE specific project files.
