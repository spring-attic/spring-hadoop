Spring Hadoop Samples
---------------------

This folder contains various various demo applications and samples for Spring Hadoop.

Please see each folder for detailed instructions (readme.txt).

As a general rule, each demo provides an integration tests that bootstraps
Hadoop, installs the demo and its dependencies and interacts with the application.

SAMPLES OVERVIEW
----------------

* wordcount
The 'traditional' Hadoop example - a word count MapReduce app configured with Spring Hadoop

* wordcount-batch
The same word count application featuring Spring Batch

* wordcount-streaming
Word count application with streaming

* wordcount-cron
Word count application with cron scheduler

* wordcount-quartz
Word count application with quartz scheduler

* wc-batch-admin
Word count application with spring batch admin

* hbase-crud-java
HBase create table, put, get, increment, scan with Java API

* hbase-mapreduce
Run map reduce with HBase

* hive-thrift-batch-admin
Run hive script with thrift API 

* hive-jdbc-batch-admin
Run hiveQL with JDBC

* pig-script-batch-admin
Run pig script with spring batch admin






BUILDING AND DEPLOYMENT
-----------------------

All demos require JDK 1.6+.

Each module can be run from its top folder using gradle wrapper:

*nix/BSD
# ../gradlew 

or

Widows
# ..\gradlew
