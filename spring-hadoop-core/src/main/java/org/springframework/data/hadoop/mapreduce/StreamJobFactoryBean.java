/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.hadoop.mapreduce;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.streaming.StreamJob;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.configuration.JobConfUtils;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;


/**
 * Factory bean focused on creating streaming jobs.
 * As opposed to {@link JobFactoryBean} which is Java-specific, this factory is suitable for streaming scenarios (such as
 * invoking Ruby/Python scripts or command-line scripts).
 *
 * @author Costin Leau
 */
public class StreamJobFactoryBean extends JobGenericOptions implements InitializingBean, FactoryBean<Job>,
		BeanNameAware {

	private Job job;
	private String name;
	private String output, mapper, reducer, combiner, inputFormat, outputFormat, partitioner;
	private Integer numReduceTasks;
	private String[] input;

	private Configuration configuration;
	private Properties properties;
	private Properties cmdEnv;

	public void setBeanName(String name) {
		this.name = name;
	}

	public Job getObject() throws Exception {
		return job;
	}

	public Class<?> getObjectType() {
		return (job != null ? job.getClass() : Job.class);
	}

	public boolean isSingleton() {
		return true;
	}

	@SuppressWarnings("deprecation")
	public void afterPropertiesSet() throws Exception {
		Assert.isTrue(!ObjectUtils.isEmpty(input), "at least one input required");
		Assert.hasText(output, "the output is required");

		final Configuration cfg = JobConfUtils.createFrom(configuration, properties);

		buildGenericOptions(cfg);

		Map<String, String> args = new LinkedHashMap<String, String>();

		// add unique arguments
		addArgument(output, "-output", args);
		addArgument(mapper, "-mapper", args);
		addArgument(reducer, "-reducer", args);
		addArgument(combiner, "-combiner", args);
		addArgument(partitioner, "-partitioner", args);
		addArgument(inputFormat, "-inputformat", args);
		addArgument(outputFormat, "-outputformat", args);

		if (numReduceTasks != null)
			addArgument(numReduceTasks.toString(), "-numReduceTasks", args);

		// translate map to list
		final List<String> argsList = new ArrayList<String>(args.size() * 2 + 16);

		for (Map.Entry<String, String> entry : args.entrySet()) {
			argsList.add(entry.getKey());
			argsList.add(entry.getValue());
		}

		// add -cmdEnv (to the list not the map to avoid key collision)
		if (cmdEnv != null) {
			Enumeration<?> props = cmdEnv.propertyNames();
			while (props.hasMoreElements()) {
				String key = props.nextElement().toString();
				argsList.add("-cmdenv");
				argsList.add(key + "=" + cmdEnv.getProperty(key));
			}
		}

		// add recurring arguments
		addArgument(input, "-input", argsList);

		if (StringUtils.hasText(user)) {
			UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				@Override
				public Void run() throws Exception {
					job = new Job(createStreamJob(cfg, argsList.toArray(new String[argsList.size()])));
					return null;
				}
			});
		}
		else {
			job = new Job(createStreamJob(cfg, argsList.toArray(new String[argsList.size()])));
		}

		job.setJobName(name);
	}

	private Configuration createStreamJob(Configuration cfg, String[] args) {
		// ugly reflection to add an extra method to #createJob
		StreamJob job = new StreamJob();
		job.setConf(cfg);
		Field argv = ReflectionUtils.findField(job.getClass(), "argv_");
		// job.argv_ = args
		ReflectionUtils.makeAccessible(argv);
		ReflectionUtils.setField(argv, job, args);

		// job.init();
		invokeMethod(job, "init");
		// job.preProcessArgs();
		invokeMethod(job, "preProcessArgs");
		// job.parseArgv();
		invokeMethod(job, "parseArgv");
		// job.postProcessArgs();
		invokeMethod(job, "postProcessArgs");
		// job.setJobConf();
		invokeMethod(job, "setJobConf");
		// return job.jobConf_;
		Field jobConf = ReflectionUtils.findField(job.getClass(), "jobConf_");
		ReflectionUtils.makeAccessible(jobConf);
		return (Configuration) ReflectionUtils.getField(jobConf, job);
	}

	private static void invokeMethod(Object target, String methodName) {
		Method m = ReflectionUtils.findMethod(target.getClass(), methodName);
		ReflectionUtils.makeAccessible(m);
		ReflectionUtils.invokeMethod(m, target);
	}

	private static void addArgument(String arg, String name, Map<String, String> args) {
		if (StringUtils.hasText(arg)) {
			args.put(name, arg.trim());
		}
	}


	static void addArgument(String[] args, String name, List<String> list) {
		if (!ObjectUtils.isEmpty(args)) {
			for (String string : args) {
				list.add(name);
				list.add(string.trim());
			}
		}
	}

	/**
	 * Sets the job input paths.
	 *
	 * @param input The input to set.
	 */
	public void setInputPath(String... input) {
		this.input = input;
	}

	/**
	 * Sets the job output paths.
	 *
	 * @param output The output to set.
	 */
	public void setOutputPath(String output) {
		this.output = output;
	}

	/**
	 * Sets the job mapper.
	 *
	 * @param mapper The mapper to set.
	 */
	public void setMapper(String mapper) {
		this.mapper = mapper;
	}

	/**
	 * Sets the job reducer.
	 * @param reducer The reducer to set.
	 */
	public void setReducer(String reducer) {
		this.reducer = reducer;
	}

	/**
	 * Sets the job combiner.
	 *
	 * @param combiner The combiner to set.
	 */
	public void setCombiner(String combiner) {
		this.combiner = combiner;
	}

	/**
	 * Sets the job input format.
	 *
	 * @param inputFormat The inputFormat to set.
	 */
	public void setInputFormat(String inputFormat) {
		this.inputFormat = inputFormat;
	}

	/**
	 * Sets the job output format.
	 *
	 * @param outputFormat The outputFormat to set.
	 */
	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}

	/**
	 * Sets the job partitioner.
	 *
	 * @param partitioner The partitioner to set.
	 */
	public void setPartitioner(String partitioner) {
		this.partitioner = partitioner;
	}

	/**
	 * Sets the Hadoop configuration to use.
	 *
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the environment for the commands to be executed.
	 *
	 * @param cmdEnv The environment command/property to set.
	 */
	public void setCmdEnv(Properties cmdEnv) {
		this.cmdEnv = cmdEnv;
	}

	/**
	 * Sets the job number of reducer tasks.
	 *
	 * @param numReduceTasks The numReduceTasks to set.
	 */
	public void setNumberReducers(Integer numReduceTasks) {
		this.numReduceTasks = numReduceTasks;
	}

	/**
	 * Sets the configuration properties to use.
	 *
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
}