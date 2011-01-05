/*
 * Copyright 2006-2011 the original author or authors.
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
package org.springframework.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.hadoop.JobTemplate;
import org.springframework.util.StringUtils;

/**
 * A factory bean for a {@link Job} to be configured with Spring and used as a
 * template to launch Hadoop processes. Instead of configuring like a native job
 * with class names for the mapper, reducer etc, this factory allows you to
 * inject concrete instances of the dependencies. Those instances will be used
 * as is for a local job or re-created from a separate application context
 * remotely for a clustered job.
 * 
 * @see JobTemplate for template usage patterns
 * 
 * @author Dave Syer
 * 
 */
public class JobFactoryBean implements FactoryBean<Job>, BeanNameAware {

	private Job job;

	private String[] inputPaths = new String[0];

	private String outputPath;

	private String name;

	private Class<?> keyClass;

	private Class<?> valueClass;

	private Reducer<?, ?, ?, ?> reducer;

	private Reducer<?, ?, ?, ?> combiner;

	private Mapper<?, ?, ?, ?> mapper;

	/**
	 * The {@link Reducer} that will be used in this job.
	 * 
	 * @return the reducer to use
	 */
	public void setReducer(Reducer<?, ?, ?, ?> reducer) {
		this.reducer = reducer;
	}

	/**
	 * The {@link Reducer combiner} that will be used in this job.
	 * 
	 * @return the combiner to use
	 */
	public void setCombiner(Reducer<?, ?, ?, ?> combiner) {
		this.combiner = combiner;
	}

	/**
	 * The {@link Mapper} that will be used in this job.
	 * 
	 * @return the mapper to use
	 */
	public void setMapper(Mapper<?, ?, ?, ?> mapper) {
		this.mapper = mapper;
	}

	/**
	 * The {@link Reducer} that will be used in this job. This public getter is
	 * provided so it can be called by a remote process to extract a mapper at
	 * run time.
	 * 
	 * @return the reducer to use
	 */
	public Reducer<?, ?, ?, ?> getReducer() {
		return reducer;
	}

	/**
	 * The {@link Reducer combiner} that will be used in this job. This public
	 * getter is provided so it can be called by a remote process to extract a
	 * mapper at run time.
	 * 
	 * @return the combiner to use
	 */
	public Reducer<?, ?, ?, ?> getCombiner() {
		return combiner;
	}

	/**
	 * The {@link Mapper} that will be used in this job. This public getter is
	 * provided so it can be called by a remote process to extract a mapper at
	 * run time.
	 * 
	 * @return the mapper to use
	 */
	public Mapper<?, ?, ?, ?> getMapper() {
		return mapper;
	}

	/**
	 * @param name
	 * @see BeanNameAware#setBeanName(java.lang.String)
	 */
	public void setBeanName(String name) {
		this.name = name;
	}

	/**
	 * @param job the job to set
	 */
	public void setJob(Job job) {
		this.job = job;
	}

	public void setInputPaths(String... inputPaths) {
		this.inputPaths = inputPaths;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public void setOutputKeyClass(Class<?> keyClass) {
		this.keyClass = keyClass;
	}

	public void setOutputValueClass(Class<?> valueClass) {
		this.valueClass = valueClass;
	}

	/**
	 * @return a {@link Job} instance
	 * @throws IOException if there is a problem creating the job
	 * @see FactoryBean#getObject()
	 */
	public Job getObject() throws IOException {
		if (job == null) {
			job = new Job();
		}
		if (inputPaths.length > 0) {
			job.getConfiguration().set(JobTemplate.SPRING_INPUT_PATHS,
					StringUtils.arrayToCommaDelimitedString(inputPaths));
		}
		if (outputPath != null) {
			job.getConfiguration().set(JobTemplate.SPRING_OUTPUT_PATH, outputPath);
		}
		if (name != null) {
			job.setJobName(name);
		}
		if (mapper != null) {
			job.setMapperClass(AutowiringMapper.class);
		}
		if (combiner != null) {
			job.setCombinerClass(AutowiringCombiner.class);
		}
		if (reducer != null) {
			job.setReducerClass(AutowiringReducer.class);
		}
		if (valueClass != null) {
			job.setOutputValueClass(valueClass);
		}
		if (keyClass != null) {
			job.setOutputKeyClass(keyClass);
		}
		return job;
	}

	/**
	 * @return {@link Job}
	 * @see FactoryBean#getObjectType()
	 */
	public Class<?> getObjectType() {
		return Job.class;
	}

	/**
	 * @return true
	 * @see FactoryBean#isSingleton()
	 */
	public boolean isSingleton() {
		return true;
	}

}
