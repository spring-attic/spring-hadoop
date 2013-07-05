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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.yarn.fs.ResourceLocalizer;
import org.springframework.yarn.support.YarnUtils;
import org.springframework.yarn.support.compat.ResourceCompat;

/**
 * Base implementation providing functionality for {@link YarnClient}.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractYarnClient implements YarnClient, InitializingBean {
	
	private final static Log log = LogFactory.getLog(AbstractYarnClient.class);

	/** Template communicating for resource manager */
	private ClientRmOperations clientRmOperations;

	/** Container request priority */
	private int priority = 0;

	/** Resource capability as of cores */
	private int virtualcores = 1;

	/** Resource capability as of memory */
	private int memory = 64;

	/** Yarn queue for the request */
	private String queue = "default";

	/** Yarn configuration for client */
	private Configuration configuration;

	/** Resource localizer for application master */
	private ResourceLocalizer resourceLocalizer;

	/** Environment for application master */
	private Map<String, String> environment;

	/** Commands starting application master */
	private List<String> commands;

	/** Name of the application */
	private String appName = "";

	/** User of the application */
	private String user;

	/** Base path for app staging directory */
	private String stagingDirPath;

	/** Name of the app specific dir name under staging dir */
	private String applicationDirName;

	/**
	 * Constructs client with a given template.
	 *
	 * @param clientRmOperations the client to resource manager template
	 */
	public AbstractYarnClient(ClientRmOperations clientRmOperations) {
		super();
		this.clientRmOperations = clientRmOperations;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(clientRmOperations, "clientRmOperations can't be null");
	}

	@Override
	public ApplicationId submitApplication() {

		// we get app id here instead in getSubmissionContext(). Otherwise
		// localizer distribute will kick off too early
		ApplicationId applicationId = clientRmOperations.getNewApplication().getApplicationId();

		// if not already set, get it from application id
		if (applicationDirName == null) {
			applicationDirName = Integer.toString(applicationId.getId());
		}

		resourceLocalizer.setStagingId(applicationDirName);
		resourceLocalizer.distribute();

		ApplicationSubmissionContext submissionContext = getSubmissionContext(applicationId);
		
		if (log.isDebugEnabled()) {
			log.debug("Using ApplicationSubmissionContext=" + submissionContext);
		}
		
		clientRmOperations.submitApplication(submissionContext);
		return applicationId;
	}

	@Override
	public void killApplication(ApplicationId applicationId) {
		clientRmOperations.killApplication(applicationId);
	}

	@Override
	public List<ApplicationReport> listApplications() {
		return clientRmOperations.listApplications();
	}

	/**
	 * Sets the {@link ClientRmOperations} implementation for
	 * accessing resource manager.
	 *
	 * @param clientRmOperations The client to resource manager implementation
	 */
	public void setClientRmOperations(ClientRmOperations clientRmOperations) {
		this.clientRmOperations = clientRmOperations;
	}

	/**
	 * Gets the client rm operations.
	 *
	 * @return the client rm operations
	 */
	public ClientRmOperations getClientRmOperations() {
		return clientRmOperations;
	}

	/**
	 * Gets the environment variables. 
	 * 
	 * @return the map of environment variables
	 */
	public Map<String, String> getEnvironment() {
		return environment;
	}
	
	/**
	 * Sets the environment for appmaster.
	 *
	 * @param environment the environment
	 */
	public void setEnvironment(Map<String, String> environment) {
		this.environment = environment;
	}

	/**
	 * Sets the commands starting appmaster.
	 *
	 * @param commands the commands starting appmaster
	 */
	public void setCommands(List<String> commands) {
		this.commands = commands;
	}

	/**
	 * Get the {@link Configuration} of this client. Internally
	 * this method is called to get the configuration which
	 * allows sub-classes to override and add additional settings.
	 * 
	 * @return the {@link Configuration}
	 */
	public Configuration getConfiguration() {
		return configuration;
	}
	
	/**
	 * Sets the Yarn configuration.
	 *
	 * @param configuration the Yarn configuration
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the resource localizer for appmaster container.
	 *
	 * @param resourceLocalizer the new resource localizer
	 */
	public void setResourceLocalizer(ResourceLocalizer resourceLocalizer) {
		this.resourceLocalizer = resourceLocalizer;
	}

	/**
	 * Sets the name for submitted application.
	 *
	 * @param appName the new application name
	 */
	public void setAppName(String appName) {
		this.appName = appName;
	}

	/**
	 * Sets the priority.
	 *
	 * @param priority the new priority
	 */
	public void setPriority(int priority) {
		this.priority = priority;
	}

	/**
	 * Sets the virtualcores.
	 *
	 * @param virtualcores the new virtualcores
	 */
	public void setVirtualcores(int virtualcores) {
		this.virtualcores = virtualcores;
	}

	/**
	 * Sets the memory.
	 *
	 * @param memory the new memory
	 */
	public void setMemory(int memory) {
		this.memory = memory;
	}

	/**
	 * Sets the queue.
	 *
	 * @param queue the new queue
	 */
	public void setQueue(String queue) {
		this.queue = queue;
	}

	/**
	 * Sets the user.
	 *
	 * @param user the new user
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * Sets the staging dir path.
	 *
	 * @param stagingDirPath the new staging dir path
	 */
	public void setStagingDirPath(String stagingDirPath) {
		this.stagingDirPath = stagingDirPath;
	}

	/**
	 * Sets the application dir name.
	 *
	 * @param applicationDirName the new application dir name
	 */
	public void setApplicationDirName(String applicationDirName) {
		this.applicationDirName = applicationDirName;
	}

	/**
	 * Gets the staging path.
	 *
	 * @return the staging path
	 */
	protected Path getStagingPath() {
		if (stagingDirPath != null && applicationDirName != null) {
			return new Path(stagingDirPath, applicationDirName);
		} else {
			return null;
		}
	}

	/**
	 * Gets the submission context for application master.
	 *
	 * @return the submission context
	 */
	protected ApplicationSubmissionContext getSubmissionContext(ApplicationId applicationId) {
		ApplicationSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
		context.setApplicationId(applicationId);
		context.setApplicationName(appName);
		context.setAMContainerSpec(getMasterContainerLaunchContext());
		if(user != null) {
			context.setUser(user);
		}
		Priority record = Records.newRecord(Priority.class);
		record.setPriority(priority);
		context.setPriority(record);
		context.setQueue(queue);
		return context;
	}

	/**
	 * Gets the master container launch context.
	 *
	 * @return the master container launch context
	 */
	protected ContainerLaunchContext getMasterContainerLaunchContext() {
		ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
		context.setLocalResources(resourceLocalizer.getResources());
		context.setEnvironment(getEnvironment());
		context.setCommands(commands);
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(memory);
		ResourceCompat.setVirtualCores(capability, virtualcores);
		context.setResource(capability);

		try {
			// TODO: this still looks a bit dodgy!!
			if (UserGroupInformation.isSecurityEnabled()) {
				Credentials credentials = new Credentials();
				final FileSystem fs = FileSystem.get(configuration);
				fs.addDelegationTokens(YarnUtils.getPrincipal(configuration), credentials);
				DataOutputBuffer dob = new DataOutputBuffer();
				credentials.writeTokenStorageToStream(dob);
				ByteBuffer containerToken  = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
				context.setContainerTokens(containerToken);
			}
		} catch (IOException e) {
		}

		return context;
	}

}
