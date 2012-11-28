The [Spring for Apache Hadoop](http://www.springsource.org/spring-data/hadoop) provides extensions to [Spring](http://www.springsource.org/spring-core), [Spring Batch](http://www.springsource.org/spring-batch), and [Spring Integration](http://www.springsource.org/spring-integration) to build manageable and robust pipeline solutions around Hadoop.  

Spring for Apache Hadoop extends Spring Batch by providing support for reading from and writing to HDFS, running various types of Hadoop jobs (Java MapReduce, Streaming, [Hive](http://hive.apache.org),  [Pig](http://pig.apache.org)), [HBase](http://hbase.apache.org) and [Cascading](http://cascading.org) interactions. An important goal is to provide excellent support for non-Java based developers to be productive using Spring Hadoop and not have to write any Java code to use the core feature set.

Spring for Apache Hadoop also applies the familiar Spring programming model to Java MapReduce jobs by providing support for dependency injection of simple jobs as well as a POJO based MapReduce programming model that decouples your MapReduce classes from Hadoop specific details such as base classes and data types.

# Docs

You can find out more details from the [user documentation](http://static.springsource.org/spring-data/hadoop/docs/current/reference/) or by browsing the [javadocs](http://static.springsource.org/spring-data/hadoop/docs/current/api/). If you have ideas about how to improve or extend the scope, please feel free to contribute.

# Artifacts

* Maven:

~~~~~ xml
<dependency>
  <groupId>org.springframework.data</groupId>
  <artifactId>spring-data-hadoop</artifactId>
  <version>${version}</version>
</dependency> 

<!-- used for nightly builds -->
<repository>
  <id>spring-maven-snapshot</id>
  <snapshots><enabled>true</enabled></snapshots>
  <name>Springframework Maven SNAPSHOT Repository</name>
  <url>http://repo.springsource.org/snapshot</url>
</repository> 

<!-- used for milestone/rc releases -->
<repository>
  <id>spring-maven-milestone</id>
  <name>Springframework Maven Milestone Repository</name>
  <url>http://repo.springsource.org/milestone</url>
</repository> 
~~~~~

* Gradle: 

Based on the artifact type, pick one of the repos below:

~~~~~ groovy
repositories {
  maven { url "http://repo.springsource.org/release" }
  maven { url "http://repo.springsource.org/milestone" }
  maven { url "http://repo.springsource.org/snapshot" }
}

dependencies {
   compile "org.springframework.data:spring-data-hadoop:${version}"
}
~~~~~

The latest milestone is _1.0.0.RC1_

The latest nightly is _1.0.0.BUILD-SNAPSHOT_

# Building

Spring for Apache Hadoop uses Gradle as its build system. To build the system simply run:

    gradlew

from the project root folder. This will compile the sources, run the tests and create the artifacts.

## Supported distros

By default Spring for Apache Hadoop compiles against Apache Hadoop 1.0.x. Apache Hadoop 1.1.x (hadoop11) and Cloudera CDH3 (cdh3) and CDH4 (cdh4) are also supported; to compile against them pass the `-P<label>` attribute:

    gradlew -Pcdh3 build
    
In this case, the specified Hadoop attributes are used to compile and create the project binaries. This option is useful when testing against dedicated Hadoop instances.

# Testing

For its testing, Spring for Apache Hadoop expects a pseudo-distributed/local Hadoop instalation available on the `localhost` at the default ports (`9000` and `9001`). The local Hadoop setup allows
the project classpath to be automatically used by the Hadoop job tracker. These settings can be customized in two ways:
* Build properties

From the command-line, use `hd.fs` for the file-system, `hd.jt` for the jobtracker and 'hd.hive` for the Hive host/port information, to override the defaults. For example to run against HDFS at `dumbo:8020` one would use:

    gradlew -Phd.fs=dumbo:8020 build

* Properties file
Through the `test.properties` file under `src/test/resources` folder (further tweaks can be applied through `hadoop-ctx.xml` file under `src/test/resources/org/springframework/data/hadoop`).

## Enabling Hbase/Hive/Pig/WebHdfs Tests
Note that by default, only the vanilla Hadoop tests are running - you can enable additional tests (such as Hive or Pig) by adding the tasks `enableHBaseTests`, `enableHiveTests`, `enablePigTests` and `webHdfsTests` (or `enableAllTests` in short). Use `test.properties` file for customizing the default location for these services as well.

## Disabling test execution
You can disable all tests by skipping the `test` task:

    gradlew -x test


# Contributing

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the [forum](http://forum.springsource.org/forumdisplay.php?87-Hadoop) by responding to questions and joining the debate.
* Create [JIRA](https://jira.springframework.org/browse/SHDP) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Watch for upcoming articles on Spring by [subscribing](http://www.springsource.org/node/feed) to springframework.org.

Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, read the Spring Framework [contributor guidelines] (https://github.com/SpringSource/spring-framework/blob/master/CONTRIBUTING.md).

# Staying in touch

Follow the project team ([Costin](http://twitter.com/costinl), [Mark](http://twitter.com/markpollack)) on Twitter. In-depth articles can be
found at the SpringSource [team blog](http://blog.springsource.org), and releases are announced via our [news feed](http://www.springsource.org/news-events).
