/*
 * Copyright 2014 the original author or authors.
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
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
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

	/** The application id. */
	private ApplicationId applicationId;

	/** Raw report from resource manager */
	private ApplicationReport applicationReport;

	/**
	 * Instantiates a new application info.
	 */
	public ApplicationInfo() {}

	/**
	 * Instantiates a new application info.
	 *
	 * @param applicationId the application id
	 * @param applicationReport the application report
	 */
	public ApplicationInfo(ApplicationId applicationId, ApplicationReport applicationReport) {
		this.applicationId = applicationId;
		this.applicationReport = applicationReport;
	}

	/**
	 * Gets the yarn application state.
	 *
	 * @return the yarn application state
	 */
	public YarnApplicationState getYarnApplicationState() {
		return applicationReport != null ? applicationReport.getYarnApplicationState() : null;
	}

	/**
	 * Gets the final application status.
	 *
	 * @return the final application status
	 */
	public FinalApplicationStatus getFinalApplicationStatus() {
		return applicationReport != null ? applicationReport.getFinalApplicationStatus() : null;
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

	/**
	 * Gets the application report.
	 *
	 * @return the application report
	 */
	public ApplicationReport getApplicationReport() {
		return applicationReport;
	}

	/**
	 * Sets the application report.
	 *
	 * @param applicationReport the new application report
	 */
	public void setApplicationReport(ApplicationReport applicationReport) {
		this.applicationReport = applicationReport;
	}

}
