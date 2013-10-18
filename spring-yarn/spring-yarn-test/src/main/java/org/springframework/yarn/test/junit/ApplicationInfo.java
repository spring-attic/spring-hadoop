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
package org.springframework.yarn.test.junit;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 * Simple holder class needed for returning multiple
 * values from {@code AbstractYarnClusterTests} app
 * submit operations.
 *
 * @author Janne Valkealahti
 *
 */
public class ApplicationInfo {

	/** The yarn application state. */
	private YarnApplicationState yarnApplicationState;

	/** The application id. */
	private ApplicationId applicationId;

	/**
	 * Instantiates a new application info.
	 */
	public ApplicationInfo() {}

	/**
	 * Instantiates a new application info.
	 *
	 * @param yarnApplicationState the yarn application state
	 * @param applicationId the application id
	 */
	public ApplicationInfo(YarnApplicationState yarnApplicationState,
			ApplicationId applicationId) {
		this.yarnApplicationState = yarnApplicationState;
		this.applicationId = applicationId;
	}

	/**
	 * Gets the yarn application state.
	 *
	 * @return the yarn application state
	 */
	public YarnApplicationState getYarnApplicationState() {
		return yarnApplicationState;
	}

	/**
	 * Sets the yarn application state.
	 *
	 * @param yarnApplicationState the new yarn application state
	 */
	public void setYarnApplicationState(YarnApplicationState yarnApplicationState) {
		this.yarnApplicationState = yarnApplicationState;
	}

	/**
	 * Gets the application id.
	 *
	 * @return the application id
	 */
	public ApplicationId getApplicationId() {
		return applicationId;
	}

	/**
	 * Sets the application id.
	 *
	 * @param applicationId the new application id
	 */
	public void setApplicationId(ApplicationId applicationId) {
		this.applicationId = applicationId;
	}

}
