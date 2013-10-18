The [Spring for Apache Hadoop](http://projects.spring.io/spring-hadoop/) project provides extensions to [Spring](http://projects.spring.io/spring-framework/), [Spring Batch](http://projects.spring.io/spring-batch/), and [Spring Integration](http://projects.spring.io/spring-integration/) to build manageable and robust pipeline solutions around Hadoop.

Spring for Apache Hadoop extends Spring Batch by providing support for reading from and writing to HDFS, running various types of Hadoop jobs (Java MapReduce, Streaming, [Hive](http://hive.apache.org),  [Pig](http://pig.apache.org)), [HBase](http://hbase.apache.org) and [Cascading](http://cascading.org) interactions. An important goal is to provide excellent support for non-Java based developers to be productive using Spring Hadoop and not have to write any Java code to use the core feature set.

Spring for Apache Hadoop also applies the familiar Spring programming model to Java MapReduce jobs by providing support for dependency injection of simple jobs as well as a POJO based MapReduce programming model that decouples your MapReduce classes from Hadoop specific details such as base classes and data types.

# Documentation

You can find out more details from the [user documentation](http://docs.spring.io/spring-hadoop/docs/current/reference/html/) or by browsing the [javadocs](http://docs.spring.io/spring-hadoop/docs/current/api/). If you have ideas about how to improve or extend the scope, please feel free to contribute.

# Artifacts

For build dependencies to use in your own projects see our [Quick Start](http://projects.spring.io/spring-hadoop/#quick-start) page.

# Building

Spring for Apache Hadoop uses Gradle as its build system. To build the system simply run:

    gradlew

from the project root folder. This will compile the sources, run the tests and create the artifacts.


## Supported distros

Spring for Apache Hadoop compiles against the following:

* Apache Hadoop 1.2.x (hadoop12) **this is the default**
* Apache Hadoop 1.0.x (hadoop10)
* Apache Hadoop 1.1.x (hadoop11)
* Apache Hadoop 2.0.x Alpha (hadoop20)
* Apache Hadoop 2.2.x (hadoop22), Cloudera CDH3 (cdh3)
* Pivotal HD 1.1 (phd1)
* Cloudera CDH4 (cdh4)
* Hortonworks HDP 1.3 (hdp13) 

to compile against them pass the `-Pdistro=<label>` project property, like so:

    gradlew -Pdistro=hadoop20 build
    
Note that the chosen distro is displayed on the screen:

    Using Apache Hadoop 1.2.x [1.2.1]

In this case, the specified Hadoop distribution (above Apache Hadoop 1.2.1) is used to create the project binaries. This option is useful when testing against Hadoop clusters incompatible with the Hadoop stable line.

## Testing notes

For its testing, Spring for Apache Hadoop expects a pseudo-distributed/local Hadoop instalation available on `localhost` configured with a port of `8020` for HDFS. The `local` Hadoop setup allows the project classpath to be automatically used by the Hadoop job tracker. These settings can be customized in two ways:


* Build properties

From the command-line, use `hd.fs` for the file-system (to avoid confusion, specify the protocol such as 'hdfs://', 's3://', etc - if none is specified, `hdfs://` will be used), `hd.jt` for the jobtracker and `hd.hive` for the Hive host/port information, to override the defaults. For example to run against HDFS at `dumbo:8020` one would use:

    gradlew -Phd.fs=hdfs://dumbo:8020 build

* Properties file

Through the `test.properties` file under `src/test/resources` folder (further tweaks can be applied through `hadoop-ctx.xml` file under `src/test/resources/org/springframework/data/hadoop`; you can also provide a yarn-site.xml with YARN specific resource manager address and classpath properties for Hadoop 2.0 based clusters).

### Enabling Hbase/Hive/Pig/WebHdfs/Cascading Tests
Note that by default, only the vanilla Hadoop tests are running - you can enable additional tests (such as Hive or Pig) by adding the tasks `enableHBaseTests`, `enableHiveTests`, `enablePigTests`, `enableWebHdfsTests` or `enableCascadingTests` (or `enableAllTests` in short). Use `test.properties` file for customizing the default location for these services as well.

### Disabling test execution
You can disable all tests by skipping the `test` task:

    gradlew -x test


# CI Builds

The results for CI builds are available at [Spring Data Hadoop: Project Summary - Spring CI](https://build.springsource.org/browse/SPRINGDATAHADOOP)


# Contributing

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the [forum](http://forum.spring.io/forum/spring-projects/data/hadoop) by responding to questions and joining the debate.
* Create [JIRA](https://jira.springframework.org/browse/SHDP) tickets for bugs and new features and comment and vote on the ones that you are interested in.

Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, read the Spring Framework [contributor guidelines] (https://github.com/spring-projects/spring-framework/blob/master/CONTRIBUTING.md).

# Staying in touch

Follow the project team ([Costin](http://twitter.com/costinl), [Mark](http://twitter.com/markpollack), [Thomas](http://twitter.com/trisberg)) on Twitter. 

In-depth articles can be found at the SpringSource [team blog](http://spring.io/blog), and releases are announced via our [news feed](http://spring.io/blog/category/news).
