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
	 * Submits the application known to {@link YarnClient} instance.
	 *
	 * @param distribute if set to true files will also be copied
	 * @return the {@link ApplicationId} for submitted application
	 */
	ApplicationId submitApplication(boolean distribute);

	/**
	 * Installs the application known to {@link YarnClient} instance into hdfs.
	 */
	void installApplication();

	/**
	 * Gets a list of known applications.
	 *
	 * @return List of {@link ApplicationReport}s
	 */
	List<ApplicationReport> listApplications();

	/**
	 * Gets a list of known applications filtered by an application type.
	 *
	 * @param type the yarn application type
	 * @return List of {@link ApplicationReport}s
	 */
	List<ApplicationReport> listApplications(String type);

	/**
	 * Gets a list of running applications filtered by an application type.
	 *
	 * @param type the yarn application type
	 * @return List of {@link ApplicationReport}s
	 */
	List<ApplicationReport> listRunningApplications(String type);

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
