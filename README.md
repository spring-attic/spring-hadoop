Spring Hadoop is a framework extension for writing Hadoop jobs that
benefit from the features of Spring, Spring Batch and Spring
Integration.  The feature set is limited right now to support for
dependency injection in simple Hadoop jobs.  If you have ideas about
how to improve or extend the scope, please feel free to contribute
(see below for instructions).

# Getting Started

Clone the source code and run `mvn install` (we use 2.1.x) from the
command line, or import as a Maven project into SpringSource Tool
Suite (STS).  There are some integration tests that show you what you
can do.  For example (from `WordCountIntegrationTests`):

    JobTemplate jobTemplate = new JobTemplate();
    jobTemplate.setVerbose(true);
    assertTrue(jobTemplate.run("/spring/autowired-job-context.xml"));

This runs a simple word count job (per the online tutorial on the
[Hadoop home
page](http://hadoop.apache.org/common/docs/r0.20.2/mapred_tutorial.html)).
Such a simple job doesn't benefit much from dependency injection, but
it shows the basic principles, and will feel very comfortable to
existing Spring users.  More complex jobs, especially with externalise
parameterisation of the components, will benefit greatly from having a
container to manage the dependencies.

The tests run out of the box in STS (or Eclipse with Maven support),
or from the command line, e.g:

    $ mvn test -Dtest=WordCountIntegrationTests

You might need to tweak your setup if you use a different IDE because
the tests use local file paths relative to the project directory.
Make sure you run the tests in an IDE launching from the project
directory (not the parent directory).

The test code above runs the job locally by default.  To run it in a
Hadoop cluster you need to run the cluster (of course) and provide
configuration for a jar file containing the job and for the job
tracker and distributed file system.  You can generate a jar file by
running `mvn assembly:single` and then using the settings already in
the test.  The test case also sets up the job tracker host for the job
configuration (the default settings will work for a local job tracker
with the settings from the [Hadoop getting started
guide](http://hadoop.apache.org/common/docs/r0.20.2/quickstart.html)).
The `HadoopSetUp` test utility is used in the test to sets up the
cluster properties if it detects the local job tracker.

# POJO Programming

With Spring Hadoop you can write business logic in Plain Old Java
Objects (POJOs), and have the framework adapt them to the underlying
API.  For example:

    public class PojoMapReducer {

        @Mapper
        public void map(String value, Map<String, Integer> writer) {
            StringTokenizer itr = new StringTokenizer(value);
            while (itr.hasMoreTokens()) {
                writer.put(itr.nextToken(), 1);
            }
        }

        @Reducer
        public int reduce(Iterable<Integer> values) {
            int sum = 0;
            for (Integer val : values) {
                sum += val;
            }
            return sum;
        }

    }

See the `PojoConfiguration` in the core tests for an example of how to
configure the POJO to run as a Hadoop `Mapper` and `Reducer`.

# Contributing to Spring Hadoop

Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/).  Before we accept a non-trivial patch or pull request we will need you to sign the [contributor's agreement](https://support.springsource.com/spring_committer_signup).  Signing the contributor's agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do.  Active contributors might be asked to join the core team, and given the ability to merge pull requests.
