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
package org.springframework.data.hadoop.configuration;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.streaming.StreamJob;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;


/**
 * Factory bean focused on creating streaming jobs.
 * As opposed to {@link JobFactoryBean} which is Java-specific, this factory is suitable for streaming scenarios (such as
 * invoking Ruby/Python scripts or command-line scripts). 
 * 
 * @author Costin Leau
 */
public class StreamJobFactoryBean implements InitializingBean, FactoryBean<Job>, BeanNameAware, ApplicationContextAware {

	private Job job;
	private String name;
	private ApplicationContext context;
	private String input, output, mapper, reducer, combiner, inputFormat, outputFormat, partitioner, cacheFile,
			cacheArchive;
	private Configuration configuration;
	private Map<String, String> env;
	private Map<String, String> arg;

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = applicationContext;
	}

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

	public void afterPropertiesSet() throws Exception {
		Configuration cfg = (configuration != null ? new Configuration(configuration) : new Configuration());

		Map<String, String> args = new LinkedHashMap<String, String>();

		// add arguments
		addArgument(input, "-input", args);
		addArgument(output, "-output", args);
		addArgument(mapper, "-mapper", args);
		addArgument(reducer, "-reducer", args);
		addArgument(combiner, "-combiner", args);
		addArgument(inputFormat, "-inputformat", args);
		addArgument(outputFormat, "-outputformat", args);
		addArgument(cacheFile, "-cacheFile", args);
		addArgument(cacheArchive, "-cacheArchive", args);

		// add cmd env
		if (env != null) {
			for (Entry<String, String> entry : env.entrySet()) {
				args.put("-cmdenv", entry.getKey() + "=" + entry.getValue());
			}
		}

		// add arg env
		if (arg != null) {
			args.putAll(arg);
		}

		// translate map to array
		String[] argsArray = new String[args.size() * 2];

		int i = -1;
		for (Map.Entry<String, String> entry : args.entrySet()) {
			argsArray[++i] = entry.getKey();
			argsArray[++i] = entry.getValue();
		}

		job = new Job(createStreamJob(cfg, argsArray));
	}

	private Configuration createStreamJob(Configuration cfg, String[] args) {
		// ugly reflection to add an extra method to #createJob
		StreamJob job = new StreamJob();
		job.setConf(cfg);
		Field argv = ReflectionUtils.findField(job.getClass(), "argv_");
		// job.argv_ = args
		ReflectionUtils.setField(argv, job, args);

		// job.init();
		ReflectionUtils.invokeMethod(ReflectionUtils.findMethod(job.getClass(), "init"), job);
		// job.preProcessArgs();
		ReflectionUtils.invokeMethod(ReflectionUtils.findMethod(job.getClass(), "preProcessArgs"), job);
		// job.parseArgv();
		ReflectionUtils.invokeMethod(ReflectionUtils.findMethod(job.getClass(), "parseArgv"), job);
		// job.postProcessArgs();
		ReflectionUtils.invokeMethod(ReflectionUtils.findMethod(job.getClass(), "postProcessArgs"), job);
		// job.setJobConf();
		ReflectionUtils.invokeMethod(ReflectionUtils.findMethod(job.getClass(), "setJobConf"), job);
		// return job.jobConf_;
		return (Configuration) ReflectionUtils.getField(ReflectionUtils.findField(job.getClass(), "jobConf_"), job);
	}

	private static void addArgument(String arg, String name, Map<String, String> args) {
		if (StringUtils.hasText(arg)) {
			args.put(name, arg.trim());
		}
	}

	/**
	 * @param input The input to set.
	 */
	public void setInput(String input) {
		this.input = input;
	}

	/**
	 * @param output The output to set.
	 */
	public void setOutput(String output) {
		this.output = output;
	}

	/**
	 * @param mapper The mapper to set.
	 */
	public void setMapper(String mapper) {
		this.mapper = mapper;
	}

	/**
	 * @param reducer The reducer to set.
	 */
	public void setReducer(String reducer) {
		this.reducer = reducer;
	}

	/**
	 * @param combiner The combiner to set.
	 */
	public void setCombiner(String combiner) {
		this.combiner = combiner;
	}

	/**
	 * @param inputFormat The inputFormat to set.
	 */
	public void setInputFormat(String inputFormat) {
		this.inputFormat = inputFormat;
	}

	/**
	 * @param outputFormat The outputFormat to set.
	 */
	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}

	/**
	 * @param partitioner The partitioner to set.
	 */
	public void setPartitioner(String partitioner) {
		this.partitioner = partitioner;
	}

	/**
	 * @param cacheFile The cacheFile to set.
	 */
	public void setCacheFile(String cacheFile) {
		this.cacheFile = cacheFile;
	}

	/**
	 * @param cacheArchive The cacheArchive to set.
	 */
	public void setCacheArchive(String cacheArchive) {
		this.cacheArchive = cacheArchive;
	}

	/**
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the environment for the commands to be executed.
	 * 
	 * @param env The env to set.
	 */
	public void setEnv(Map<String, String> env) {
		this.env = env;
	}

	/**
	 * Sets additional arguments to be used for the streaming job.
	 * @param arg The arg to set.
	 */
	public void setArg(Map<String, String> arg) {
		this.arg = arg;
	}
}