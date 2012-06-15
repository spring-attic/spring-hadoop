/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.cascading;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;

import cascading.cascade.Cascade;
import cascading.flow.hadoop.MapReduceFlow;

/**
 * Factory for declarative {@link MapReduceFlow} creation. Usually used with a {@link Cascade}. 
 * 
 * Note the flow is not started.
 * @author Costin Leau
 */
public class MapReduceFlowFactoryBean extends FlowFactoryBean<MapReduceFlow> implements BeanNameAware {

	private Configuration configuration;
	private Properties properties;
	private Job job;

	private String beanName;
	private boolean deleteSinkOnInit = false;


	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}


	@Override
	MapReduceFlow createFlow() {
		Configuration c = configuration;
		if (job != null) {
			c = ConfigurationUtils.merge(configuration, job.getConfiguration());
		}

		Configuration conf = ConfigurationUtils.createFrom(c, properties);
		JobConf jobConf = new JobConf(conf);
		return new MapReduceFlow(beanName, jobConf, deleteSinkOnInit, false);
	}

	/**
	 * Sets the Hadoop job (as an alternative to setting a configuration).
	 *
	 * @param job The job to set.
	 */
	public void setJob(Job job) {
		this.job = job;
	}

	/**
	 * Sets the configuration.
	 *
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}


	/**
	 * Sets the properties.
	 *
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	/**
	 * Sets the delete sink on init.
	 *
	 * @param deleteSinkOnInit The deleteSinkOnInit to set.
	 */
	public void setDeleteSinkOnInit(boolean deleteSinkOnInit) {
		this.deleteSinkOnInit = deleteSinkOnInit;
	}
}