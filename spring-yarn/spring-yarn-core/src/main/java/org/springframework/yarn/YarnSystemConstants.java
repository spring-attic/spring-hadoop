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
package org.springframework.yarn;

/**
 * Various contants throughout the Spring Yarn libraries.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnSystemConstants {

	/** Default bean id for appmaster. */
	public static final String DEFAULT_ID_APPMASTER = "yarnAppmaster";

	/** Default bean id for container. */
	public static final String DEFAULT_ID_CONTAINER = "yarnContainer";

	/** Default bean id for client. */
	public static final String DEFAULT_ID_CLIENT = "yarnClient";

	/** Default bean id for resource localizer. */
	public static final String DEFAULT_ID_LOCAL_RESOURCES = "yarnLocalresources";

	/** Default bean id for resource yarn configuration. */
	public static final String DEFAULT_ID_CONFIGURATION = "yarnConfiguration";

	/** Default bean id for resource environment. */
	public static final String DEFAULT_ID_ENVIRONMENT = "yarnEnvironment";

	/** Default bean id for appmaster service. */
	public static final String DEFAULT_ID_AMSERVICE = "yarnAmservice";

	/** Default bean id for Yarn event publisher. */
	public static final String DEFAULT_ID_EVENT_PUBLISHER = "yarnEventPublisher";

	/** Default bean id for appmaster service client. */
	public static final String DEFAULT_ID_AMSERVICE_CLIENT = "yarnAmserviceClient";

	/** Default env variable for amservice port. */
	public static final String AMSERVICE_PORT = "syarn.amservice.port";

	/** Default env variable for amservice host. */
	public static final String AMSERVICE_HOST = "syarn.amservice.host";

	/** Default env variable for amservice batch step name. */
	public static final String AMSERVICE_BATCH_STEPNAME = "syarn.amservice.batch.stepname";

	/** Default env variable for amservice batch job execution id. */
	public static final String AMSERVICE_BATCH_JOBEXECUTIONID = "syarn.amservice.batch.jobexecutionid";

	/** Default env variable for amservice batch step execution id. */
	public static final String AMSERVICE_BATCH_STEPEXECUTIONID = "syarn.amservice.batch.stepexecutionid";

	/** Env variable for container id. */
	public static final String SYARN_CONTAINER_ID = "syarn.containerid";

	/** Default staging directory base name. */
	public static final String DEFAULT_STAGING_BASE_DIR_NAME = "syarn";

	/** Default staging directory name. */
	public static final String DEFAULT_STAGING_DIR_NAME = "staging";

}
