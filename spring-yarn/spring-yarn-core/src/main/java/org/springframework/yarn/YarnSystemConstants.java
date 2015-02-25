/*
 * Copyright 2013-2015 the original author or authors.
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

	/** Default bean id for container class. */
	public static final String DEFAULT_ID_CONTAINER_CLASS = "yarnContainerClass";

	/** Default bean id for container ref. */
	public static final String DEFAULT_ID_CONTAINER_REF = "yarnContainerRef";

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

	/** Default bean id for appmaster track service. */
	public static final String DEFAULT_ID_AMTRACKSERVICE = "yarnAmTrackservice";

	/** Default bean id for Yarn event publisher. */
	public static final String DEFAULT_ID_EVENT_PUBLISHER = "yarnEventPublisher";

	/** Default bean id for Yarn container shutdown. */
	public static final String DEFAULT_CONTAINER_SHUTDOWN = "yarnContainerShutdown";

	/** Default bean id for appmaster service client. */
	public static final String DEFAULT_ID_AMSERVICE_CLIENT = "yarnAmserviceClient";

	/** Default bean id for appmaster client service. */
	public static final String DEFAULT_ID_CLIENT_AMSERVICE = "yarnClientAmservice";

	/** Default bean id for yarn specific rest template. */
	public static final String DEFAULT_ID_RESTTEMPLATE = "yarnRestTemplate";

	/** Default name of client context file. */
	public static final String DEFAULT_CONTEXT_FILE_CLIENT = "application-context.xml";

	/** Default name of appmaster context file. */
	public static final String DEFAULT_CONTEXT_FILE_APPMASTER = "appmaster-context.xml";

	/** Default name of container context file. */
	public static final String DEFAULT_CONTEXT_FILE_CONTAINER = "container-context.xml";

	/** Default env variable for track url. */
	public static final String AMSERVICE_TRACKURL = "SHDP_AMSERVICE_TRACKURL";

	/** Default env variable for amservice port. */
	public static final String AMSERVICE_PORT = "SHDP_AMSERVICE_PORT";

	/** Default env variable for amservice host. */
	public static final String AMSERVICE_HOST = "SHDP_AMSERVICE_HOST";

	/** Default env variable for amservice batch step name. */
	public static final String AMSERVICE_BATCH_STEPNAME = "SHDP_AMSERVICE_BATCH_STEPNAME";

	/** Default env variable for amservice batch step execution name. */
	public static final String AMSERVICE_BATCH_STEPEXECUTIONNAME = "SHDP_AMSERVICE_BATCH_STEPEXECUTIONNAME";

	/** Default env variable for amservice batch job execution id. */
	public static final String AMSERVICE_BATCH_JOBEXECUTIONID = "SHDP_AMSERVICE_BATCH_JOBEXECUTIONID";

	/** Default env variable for amservice batch step execution id. */
	public static final String AMSERVICE_BATCH_STEPEXECUTIONID = "SHDP_AMSERVICE_BATCH_STEPEXECUTIONID";

	/** Env variable for container id. */
	public static final String SYARN_CONTAINER_ID = "SHDP_CONTAINERID";

	/** Default staging directory base name. */
	public static final String DEFAULT_STAGING_BASE_DIR_NAME = "syarn";

	/** Default staging directory name. */
	public static final String DEFAULT_STAGING_DIR_NAME = "staging";

	/** Default env variable for resource manager address. */
	public static final String RM_ADDRESS = "SHDP_HD_RM";

	/** Default env variable for hdfs address. */
	public static final String FS_ADDRESS = "SHDP_HD_FS";

	/** Default env variable for resource manager scheduler address. */
	public static final String SCHEDULER_ADDRESS = "SHDP_HD_SCHEDULER";

}
