/*
 * Copyright 2006-2010 the original author or authors.
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
package org.springframework.hadoop;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.hadoop.context.DefaultContextLoader;
import org.springframework.hadoop.context.HadoopApplicationContextUtils;
import org.springframework.hadoop.util.PropertiesUtils;
import org.springframework.util.StringUtils;

public class JobTemplate {

	/**
	 * The default location for a job configuration.
	 */
	public static final String DEFAULT_CONFIG_LOCATION = "classpath*:/META-INF/spring/hadoop/job-context.xml";

	public static final String SPRING_INPUT_PATHS = "spring.input.paths";

	public static final String SPRING_OUTPUT_PATH = "spring.output.path";

	private boolean verbose = false;

	private String jarFile;

	private String hostname;

	private int port = 9001;

	private Properties bootstrapProperties = new Properties();

	private Properties extraConfiguration = new Properties();

	private Configuration configuration = new Configuration();

	/**
	 * @param verbose the verbose flag to set
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * @param configuration extra configuration to apply
	 */
	public void setExtraConfiguration(Properties configuration) {
		this.extraConfiguration = configuration;
	}

	/**
	 * @param configuration configuration to apply
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * @param bootstrapProperties the bootstrap properties to set
	 */
	public void setBootstrapProperties(Properties bootstrapProperties) {
		this.bootstrapProperties = bootstrapProperties;
	}

	public void setJarFile(String jarFile) {
		this.jarFile = jarFile;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public boolean run() {
		return run(DEFAULT_CONFIG_LOCATION);
	}

	// TODO: add callback to enhance context?
	public boolean run(String configLocation) {
		Job template = HadoopApplicationContextUtils.getJob(configLocation, getBootstrapConfiguration());
		return runFromTemplate(template);
	}

	public boolean run(String configLocation, String jobName) {
		Job template = HadoopApplicationContextUtils.getJob(configLocation, getBootstrapConfiguration(), jobName);
		return runFromTemplate(template);
	}

	public boolean run(Class<?> configLocation) {
		Job template = HadoopApplicationContextUtils.getJob(configLocation);
		return runFromTemplate(template);
	}

	public boolean run(Class<?> configLocation, String jobName) {
		Job template = HadoopApplicationContextUtils.getJob(configLocation, getBootstrapConfiguration(), jobName);
		return runFromTemplate(template);
	}

	public ClusterStatus getClusterStatus() {
		try {
			// TODO: is there a non-deprecated way to do this?
			@SuppressWarnings("deprecation")
			JobClient client = new JobClient(new org.apache.hadoop.mapred.JobConf(getBootstrapConfiguration()));
			return client.getClusterStatus();
		}
		catch (IOException e) {
			throw new HadoopException("Cannot get cluster status", e);
		}
	}

	private Configuration getBootstrapConfiguration() {
		mergeExtraConfiguration(configuration);
		configuration.set(DefaultContextLoader.SPRING_CONFIG_BOOTSTRAP, PropertiesUtils.propertiesToString(bootstrapProperties));
		return configuration;
	}

	private boolean runFromTemplate(Job template) {
		return runFromTemplate(template, null);
	}

	private boolean runFromTemplate(Job template, Properties bootstrap) {

		try {

			// Construct the new complete configuration *before* the Job is
			// initialized...
			Configuration configuration = template.getConfiguration();
			mergeExtraConfiguration(configuration);
			// Leave the original Job intact (so it can be a singleton).
			Job job = new Job(configuration, template.getJobName());

			if (configuration.get(SPRING_INPUT_PATHS) != null) {
				for (String path : StringUtils.commaDelimitedListToStringArray(configuration.get(SPRING_INPUT_PATHS))) {
					FileInputFormat.addInputPath(job, new Path(path));
				}
			}
			if (configuration.get(SPRING_OUTPUT_PATH) != null) {
				String path = configuration.get(SPRING_OUTPUT_PATH);
				FileOutputFormat.setOutputPath(job, new Path(path));
			}

			return job.waitForCompletion(verbose);

		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new HadoopException("Cannot execute Job", e);
		}
		finally {
			HadoopApplicationContextUtils.releaseJob(template);
		}

	}

	private void mergeExtraConfiguration(Configuration configuration) {
		for (Entry<Object, Object> entry : getExtraConfiguration().entrySet()) {
			configuration.set((String) entry.getKey(), (String) entry.getValue());
		}
	}

	public Properties getExtraConfiguration() {
		Properties map = new Properties();
		map.putAll(extraConfiguration);
		if (jarFile != null) {
			map.setProperty("mapred.jar", jarFile);
		}
		if (hostname != null) {
			map.setProperty("mapred.job.tracker", String.format("%s:%d", hostname, port));
			// TODO: make fs port configurable
			map.setProperty("fs.default.name", String.format("hdfs://%s:%d/", hostname, port - 1));
		}
		return map;
	}

	public void addExtraConfiguration(String key, String value) {
		extraConfiguration.put(key, value);
	}

}
