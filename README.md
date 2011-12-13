The [Spring Hadoop](http://www.springsource.org/spring-data/hadoop) provides extensions to [Spring](http://www.springsource.org/spring-core), [Spring Batch](http://www.springsource.org/spring-batch), and [Spring Integration](http://www.springsource.org/spring-integration) to build manageable and robust pipeline solutions around Hadoop.  

Spring Hadoop extends Spring Batch by providing support for reading from and writing to HDFS, running various types of Hadoop jobs (Java MapReduce, Streaming, [Hive](http://hive.apache.org),  [Pig](http://pig.apache.org)) and [HBase](http://hbase.apache.org) interactions. An important goal is to provide excellent support for non-Java based developers to be productive using Spring Hadoop and not have to write any Java code to use the core feature set.

Spring Hadoop also applies the familiar Spring programming model to Java MapReduce jobs by providing support for dependency injection of simple jobs as well as a POJO based MapReduce programming model that decouples your MapReduce classes from Hadoop specific details such as base classes and data types.

# Docs

You can find out more details from the [user documentation](http://static.springsource.org/spring-data/hadoop/docs/current/reference/) or by browsing the [javadocs](http://static.springsource.org/spring-data/hadoop/docs/current/api/). If you have ideas about how to improve or extend the scope, please feel free to contribute.

# Artifacts

* Maven:

      <dependency>
        <groupId>org.springframework.data</groupId>
        <artifactId>spring-data-hadoop</artifactId>
        <version>1.0.0.BUILD-SNAPSHOT</version>
      </dependency> 


      <repository>
        <id>spring-maven-snapshot</id>
        <snapshots><enabled>true</enabled></snapshots>
        <name>Springframework Maven SNAPSHOT Repository</name>
        <url>http://maven.springframework.org/snapshot</url>
      </repository> 

* Gradle

    repositories {
       mavenRepo name: "spring-snapshot", urls: "http://maven.springframework.org/snapshot"
    }
    
    dependencies {
       compile "org.springframework.data:spring-data-hadoop:1.0.0.BUILD-SNAPSHOT
    }

# Contributing to Spring Hadoop

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the [forum](http://forum.springsource.org/forumdisplay.php?f=80) by responding to questions and joining the debate.
Please add 'Hadoop' as a prefix to easily spot the post topic.
* Create [JIRA](https://jira.springframework.org/browse/SHDP) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Watch for upcoming articles on Spring by [subscribing](http://www.springsource.org/node/feed) to springframework.org

Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, please reference a tracker ticket as well covering the specific issue you are addressing. Before we accept a non-trivial patch or pull request we will need you to sign the [contributor's agreement](https://support.springsource.com/spring_committer_signup).  Signing the contributor's agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do.  Active contributors might be asked to join the core team, and given the ability to merge pull requests.

# Building Spring Hadoop

Spring Hadoop uses Gradle as its build system. To build the system simply run:

    gradlew

from the project root folder. This will compile the sources, run the tests and create the artifacts. 
Note that by default, only the vanilla Hadoop tests are running - you can enable additional tests by adding the tasks `enableHBaseTests`, `enableHiveTests` and `enablePigTests` (or `enableAllTests` in short).
You can disable all tests by skipping the `test` task:

    gradlew -x test



    
