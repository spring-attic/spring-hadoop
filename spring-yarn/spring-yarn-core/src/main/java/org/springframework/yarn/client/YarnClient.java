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
package org.springframework.yarn.client;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

/**
 * Interface for Spring Yarn facing client methods.
 *
 * @author Janne Valkealahti
 *
 */
public interface YarnClient {

	/**
	 * Submits the application known to {@link YarnClient} instance.
	 *
	 * @return the {@link ApplicationId} for submitted application
	 */
	ApplicationId submitApplication();

	/**
	 * Gets a list of known applications.
	 *
	 * @return List of {@link ApplicationReport}s
	 */
	List<ApplicationReport> listApplications();

	/**
	 * Requests a resource manager to kill the application.
	 *
	 * @param applicationId the {@link ApplicationId}
	 */
	void killApplication(ApplicationId applicationId);

	/**
	 * Gets a report of the application.
	 * 
	 * @param applicationId the application id
	 * 
	 * @return the {@link ApplicationReport}
	 */
	ApplicationReport getApplicationReport(ApplicationId applicationId);

}
