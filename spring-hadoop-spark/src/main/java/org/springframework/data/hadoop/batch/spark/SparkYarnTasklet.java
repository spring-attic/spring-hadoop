/*
 * Copyright 2015 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.batch.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Spark tasklet running Spark jobs on demand on YARN cluster.
 * 
 * @author Thomas Risberg
 */
public class SparkYarnTasklet implements InitializingBean, Tasklet, StepExecutionListener {

	private boolean complete = false;

	private String sparkAssemblyJar;

	private String extraClassPath;

	private Configuration hadoopConfiguration;

	private String appName;

	private String appClass;

	private String appJar;

	private String resourceFiles;

	private String resourceArchives;

	private String executorMemory;

	private String executorCores;

	private String driverMemory;

	private String driverCores;

	private int numExecutors;

	private String[] arguments;

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		SparkConf sparkConf = new SparkConf();
		sparkConf.set("spark.yarn.jar", sparkAssemblyJar);
		if (StringUtils.hasText(extraClassPath)) {
			sparkConf.set("spark.driver.extraClassPath", extraClassPath);
			sparkConf.set("spark.executor.extraClassPath", extraClassPath);
		}
		List<String> submitArgs = new ArrayList<String>();
		if (StringUtils.hasText(appName)) {
			submitArgs.add("--name");
			submitArgs.add(appName);
		}
		submitArgs.add("--jar");
		submitArgs.add(appJar);
		submitArgs.add("--class");
		submitArgs.add(appClass);
		if (StringUtils.hasText(resourceFiles)) {
			submitArgs.add("--files");
			submitArgs.add(resourceFiles);
		}
		if (StringUtils.hasText(resourceArchives)) {
			submitArgs.add("--archives");
			submitArgs.add(resourceArchives);
		}
		submitArgs.add("--num-executors");
		submitArgs.add("" + numExecutors);
		submitArgs.add("--executor-memory");
		submitArgs.add(executorMemory);
		if (!StringUtils.hasText(executorCores)) {
			submitArgs.add("--executor-cores");
			submitArgs.add(executorCores);
		}
		if (!StringUtils.hasText(driverMemory)) {
			submitArgs.add("--driver-memory");
			submitArgs.add(driverMemory);
		}
		if (!StringUtils.hasText(driverCores)) {
			submitArgs.add("--driver-cores");
			submitArgs.add(driverCores);
		}
		for (String arg : arguments) {
			submitArgs.add("--arg");
			submitArgs.add(arg);
		}
		ClientArguments clientArguments =
				new ClientArguments(submitArgs.toArray(new String[submitArgs.size()]), sparkConf);
		Client client = new Client(clientArguments, hadoopConfiguration, sparkConf);
		System.setProperty("SPARK_YARN_MODE", "true");
		client.run();
		complete = true;
		return RepeatStatus.FINISHED;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {

	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (complete) {
			return ExitStatus.COMPLETED;
		}
		else {
			return ExitStatus.FAILED;
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.hasText(sparkAssemblyJar, "sparkAssemblyJar property was not set. " +
				"You must specify the path for the spark-assembly jar file. " +
				"It can either be a local file or stored in HDFS using an 'hdfs://' prefix.");
		Assert.notNull(hadoopConfiguration, "hadoopConfiguration property was not set. " +
				"You must provide a reference to the Hadoop configuration to be used.");
		Assert.hasText(appClass, "appClass property was not set. " +
				"You must specify the main class of the application to execute.");
		Assert.hasText(appJar, "appJar property was not set." +
				"You must specify the path to the jar that contains the app to execute.");
		if (!StringUtils.hasText(executorMemory)) {
			executorMemory = "1G";
		}
		if (numExecutors == 0) {
			numExecutors = 1;
		}
	}

	public void setSparkAssemblyJar(String sparkAssemblyJar) {
		this.sparkAssemblyJar = sparkAssemblyJar;
	}

	public void setExtraClassPath(String extraClassPath) {
		this.extraClassPath = extraClassPath;
	}

	public void setHadoopConfiguration(Configuration configuration) {
		this.hadoopConfiguration = configuration;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public void setAppClass(String appClass) {
		this.appClass = appClass;
	}

	public void setAppJar(String appJar) {
		this.appJar = appJar;
	}

	public void setResourceFiles(String resourceFiles) {
		this.resourceFiles = resourceFiles;
	}

	public void setResourceArchives(String resourceArchives) {
		this.resourceArchives = resourceArchives;
	}

	public void setExecutorMemory(String executorMemory) {
		this.executorMemory = executorMemory;
	}

	public void setExecutorCores(String executorCores) {
		this.executorCores = executorCores;
	}

	public void setDriverMemory(String driverMemory) {
		this.driverMemory = driverMemory;
	}

	public void setDriverCores(String driverCores) {
		this.driverCores = driverCores;
	}

	public void setNumExecutors(int numExecutors) {
		this.numExecutors = numExecutors;
	}

	public void setArguments(String[] arguments) {
		this.arguments = arguments;
	}
}
