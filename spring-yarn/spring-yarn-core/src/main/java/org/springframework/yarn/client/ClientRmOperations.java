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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 * Interface for client to resource manager communication.
 *
 * @author Janne Valkealahti
 * @see org.springframework.yarn.client.ClientRmTemplate
 */
public interface ClientRmOperations {

	/**
	 * Requests a new application from a resource manager. Main purpose is to
	 * get a new {@link org.apache.hadoop.yarn.api.records.ApplicationId} but response
	 * also contains information about resource capabilities.
	 *
	 * @return the new {@link GetNewApplicationResponse}
	 */
	GetNewApplicationResponse getNewApplication();

	/**
	 * Submits a new application into resource manager. Returned response
	 * is an empty placeholder, thus application submission is considered
	 * to be successful if no exceptions are thrown.
	 *
	 * @param appSubContext the Application Submission Context
	 * @return the new {@link SubmitApplicationResponse}
	 */
	SubmitApplicationResponse submitApplication(ApplicationSubmissionContext appSubContext);

	/**
	 * Gets a list of {@link ApplicationReport}s from a resource manager.
	 *
	 * @return a list of {@link ApplicationReport}s
	 */
	List<ApplicationReport> listApplications();

	/**
	 * Gets a list of {@link ApplicationReport}s from a resource manager. Allows
	 * to filter results by using a set of {@code YarnApplicationState}s and
	 * registered application types.
	 *
	 * @param states the yarn application states
	 * @param types the yarn application types
	 * @return a list of {@link ApplicationReport}s
	 */
	List<ApplicationReport> listApplications(EnumSet<YarnApplicationState> states, Set<String> types);

	/**
	 * Requests <code>ResourceManager</code> to abort submitted application.
	 *
	 * @param applicationId the application id
	 * @return the {@link KillApplicationResponse}
	 */
	KillApplicationResponse killApplication(ApplicationId applicationId);

	/**
	 * Gets the resource manager delegation token.
	 *
	 * @param renewer the renewer as kerberos principal
	 * @return the delegation token
	 */
	Token getDelegationToken(String renewer);

	/**
	 * Gets a report of the application.
	 *
	 * @param applicationId the application id
	 *
	 * @return the {@link ApplicationReport}
	 */
	ApplicationReport getApplicationReport(ApplicationId applicationId);

}
