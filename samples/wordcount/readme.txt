====================
== WordCount Demo ==
====================

1. MOTIVATION

A basic word count demo that illustrates the configuration and interaction with the Hadoop through Spring Hadoop.
The demo copies local resources into HDFS and executes the Hadoop example that counts words against it. The demo
requires a running Hadoop instance (by default at localhost:9000). 
The Hadoop settings can be configured through hadoop.properties (more info in the Spring Hadoop reference docs).

2. BUILD AND DEPLOYMENT

This directory contains the source files.
For building, JDK 1.6+ are required

a) To build and run the sample as a JUnit test, use the following command:

*nix/BSD OS:
$ ../../gradlew

Windows OS:
$ ..\..\gradlew

If you have Gradle installed and available in your classpath, you can simply type:
$ gradle

The rest of the document refers to only the *nix/BSD OS style command line

b) To build and run the sample as a standalone Java application:

$ ../../gradlew installApp
$ ./build/install/wordcount/bin/wordcount classpath:/launch-context.xml job1

This uses the CommandLineJobRunner launcher from the Spring Batch project. 

4. Importing sample code to eclipse

run the command

$ ../../gradlew eclipse

This will generate an eclipse .profile file that you can import into eclipse.
