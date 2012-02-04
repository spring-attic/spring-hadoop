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

* batch-wordcount
The same word count application featuring Spring Batch


BUILDING AND DEPLOYMENT
-----------------------

All demos require JDK 1.6+.

Each module can be run from its top folder using gradle wrapper:

*nix/BSD
# ../gradlew 

or

Widows
# ..\gradlew
