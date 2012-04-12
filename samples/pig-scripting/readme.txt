======================
== Pig Script Demo  ==
======================

1. MOTIVATION

The demo showcasing how to use Pig with Spring Hadoop. The demo requires a running Hadoop instance (by default at localhost:9000). 
The Hadoop settings can be configured through hadoop.properties (more info in the Spring Hadoop reference docs).

2. BUILD AND DEPLOYMENT

This directory contains the source files.
For building, JDK 1.6+ are required

a) To build, test and run the sample, use the following command:

*nix/BSD OS:
$ ../gradlew

Windows OS:
$ ..\gradlew

If you have Gradle installed and available in your classpath, you can simply type:
$ gradle

This launches the Spring Batch CommandLineJobRunner and triggers the job.

3. IDE IMPORT

To import the code inside an IDE run the command

For Eclipse 
$ ../gradlew eclipse

For IDEA
$ ../gradlew idea

This will generate the IDE specific project files.
