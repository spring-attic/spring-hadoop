================================
== Batch Admin WordCount Demo ==
================================

1. MOTIVATION

A basic word count demo that illustrates the configuration and interaction with the Hadoop through Spring Hadoop
and Spring Batch Admin. The demo copies local resources into HDFS and executes the Hadoop example that counts words 
against it. The demo requires a running Hadoop instance (by default at localhost:9000). 
The Hadoop settings can be configured through hadoop.properties (more info in the Spring Hadoop reference docs).

2. BUILD AND DEPLOYMENT

This directory contains the source files.
For building, JDK 1.6+ are required

a) To build and run the sample as a JUnit test, use the following command:

*nix/BSD OS:
$ ../gradlew

Windows OS:
$ ..\gradlew

If you have Gradle installed and available in your classpath, you can simply type:
$ gradle

The rest of the document refers to only the *nix/BSD OS style command line


b) To build and run the sample with Spring Batch Admin:

$ ../gradlew jettyRun

then open internet explorer and input "http://localhost:8081/wc-batch-admin", click "Jobs" -> "job1". In the Job Parameters text field,
 input "fail=false run.id=1", and click "Launch" button.

3. IDE IMPORT

To import the code inside an IDE run the command

For Eclipse 
$ ../gradlew eclipse

For IDEA
$ ../gradlew idea

This will generate the IDE specific project files.
