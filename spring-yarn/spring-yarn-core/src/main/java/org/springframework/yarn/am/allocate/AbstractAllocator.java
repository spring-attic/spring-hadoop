/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.am.allocate;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.springframework.yarn.am.AppmasterRmOperations;
import org.springframework.yarn.am.AppmasterRmTemplate;
import org.springframework.yarn.support.LifecycleObjectSupport;

/**
 * The base class for Container allocator implementations.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractAllocator extends LifecycleObjectSupport {

	/** Yarn configuration */
	private Configuration configuration;

	/** Environment variables for the process */
	private Map<String, String> environment;

	/** Per application attempt id */
	private ApplicationAttemptId applicationAttemptId;

	/** Operations template talking to resource manager */
	private AppmasterRmOperations rmTemplate;

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		AppmasterRmTemplate armt = new AppmasterRmTemplate(getConfiguration());
		armt.afterPropertiesSet();
		rmTemplate = armt;
	}

	/**
	 * Gets the application attempt id.
	 *
	 * @return the application attempt id
	 */
	public ApplicationAttemptId getApplicationAttemptId() {
		return applicationAttemptId;
	}

	/**
	 * Sets the application attempt id.
	 *
	 * @param applicationAttemptId the new application attempt id
	 */
	public void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
		this.applicationAttemptId = applicationAttemptId;
	}

	/**
	 * Gets the Yarn configuration.
	 *
	 * @return the Yarn configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Sets the Yarn configuration.
	 *
	 * @param configuration the new Yarn configuration
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Gets the environment.
	 *
	 * @return the environment
	 */
	public Map<String, String> getEnvironment() {
		return environment;
	}

	/**
	 * Sets the environment.
	 *
	 * @param environment the environment
	 */
	public void setEnvironment(Map<String, String> environment) {
		this.environment = environment;
	}

	/**
	 * Sets the rm template.
	 *
	 * @param rmTemplate the new rm template
	 */
	public void setRmTemplate(AppmasterRmOperations rmTemplate) {
		this.rmTemplate = rmTemplate;
	}

	/**
	 * Gets the rm template.
	 *
	 * @return the rm template
	 */
	public AppmasterRmOperations getRmTemplate() {
		return rmTemplate;
	}

}
