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

import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

/**
 * Factory for creating a {@link PigContext} instance.
 * 
 * @author Costin Leau
 */
public class PigContextFactoryBean implements InitializingBean, FactoryBean<PigContext> {

	private PigContext context;

	private String lastAlias;
	private String jobTracker;
	private ExecType execType = ExecType.MAPREDUCE;
	private Properties properties = new Properties();

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
		if (StringUtils.hasText(jobTracker)) {
			properties.setProperty("mapred.job.tracker", jobTracker);
			// invoking setter below causes NPE since PIG expects the engine to be started already ...
			// context.setJobtrackerLocation(jobTracker);
		}
		context = new PigContext(execType, properties);

		if (StringUtils.hasText(lastAlias)) {
			context.setLastAlias(lastAlias);
		}
	}

	/**
	 * @param lastAlias The lastAlias to set.
	 */
	public void setLastAlias(String lastAlias) {
		this.lastAlias = lastAlias;
	}

	/**
	 * @param jobTracker The jobTracker to set.
	 */
	public void setJobTracker(String jobTracker) {
		this.jobTracker = jobTracker;
	}

	/**
	 * @param execType The execType to set.
	 */
	public void setExecType(ExecType execType) {
		this.execType = execType;
	}

	/**
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
}