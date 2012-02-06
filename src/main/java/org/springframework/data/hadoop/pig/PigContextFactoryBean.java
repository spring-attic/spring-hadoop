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
package org.springframework.data.hadoop.pig;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

/**
 * Factory for creating a {@link PigContext} instance. Used for detailed configuration of a {@link PigServer} instance.
 * 
 * @author Costin Leau
 */
public class PigContextFactoryBean implements InitializingBean, FactoryBean<PigContext> {

	private PigContext context;

	private String lastAlias;
	private String jobTracker;
	private ExecType execType = ExecType.MAPREDUCE;
	private Properties properties;
	private Configuration configuration;

	public PigContext getObject() throws Exception {
		return context;
	}

	public Class<?> getObjectType() {
		return (context != null ? context.getClass() : PigContext.class);
	}

	public boolean isSingleton() {
		return true;
	}

	public void afterPropertiesSet() throws Exception {
		Properties prop = new Properties();

		// first add the hadoop config
		if (configuration != null) {
			Iterator<Entry<String, String>> iterator = configuration.iterator();
			while (iterator.hasNext()) {
				Map.Entry<java.lang.String, java.lang.String> entry = iterator.next();
				prop.setProperty(entry.getKey(), entry.getValue());
			}
		}

		// add properties
		if (properties != null) {
			Enumeration<?> names = properties.propertyNames();
			while (names.hasMoreElements()) {
				String name = (String) names.nextElement();
				prop.setProperty(name, properties.getProperty(name));
			}
		}

		if (StringUtils.hasText(jobTracker)) {
			prop.setProperty("mapred.job.tracker", jobTracker);
			// invoking setter below causes NPE since PIG expects the engine to be started already ...
			// context.setJobtrackerLocation(jobTracker);
		}
		context = new PigContext(execType, prop);

		if (StringUtils.hasText(lastAlias)) {
			context.setLastAlias(lastAlias);
		}
	}

	/**
	 * Sets the last alias.
	 * 
	 * @param lastAlias The lastAlias to set.
	 */
	public void setLastAlias(String lastAlias) {
		this.lastAlias = lastAlias;
	}

	/**
	 * Sets the job tracker.
	 * 
	 * @param jobTracker The jobTracker to set.
	 */
	public void setJobTracker(String jobTracker) {
		this.jobTracker = jobTracker;
	}

	/**
	 * Sets the execution type.
	 * 
	 * @param execType The execType to set.
	 */
	public void setExecType(ExecType execType) {
		this.execType = execType;
	}

	/**
	 * Sets the configuration properties.
	 * 
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	/**
	 * Sets the Hadoop configuration to use.
	 * 
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}
}