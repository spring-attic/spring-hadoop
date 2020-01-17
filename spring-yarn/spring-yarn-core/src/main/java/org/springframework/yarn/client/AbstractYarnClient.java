/*
 * Copyright 2013-2016 the original author or authors.
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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.yarn.fs.ResourceLocalizer;
import org.springframework.yarn.fs.SmartResourceLocalizer;
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
	private long memory = 64;

	/** Yarn queue for the request */
	private String queue = "default";

	/** Application label expression */
	private String labelExpression;

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

	/** Type of the application */
	private String appType;

	/** Base path for app staging directory */
	private String stagingDirPath;

	/** App specific dir name under staging dir */
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
		return submitApplication(true);
	}

	@Override
	public ApplicationId submitApplication(boolean distribute) {

		// we get app id here instead in getSubmissionContext(). Otherwise
		// localizer distribute will kick off too early
		ApplicationId applicationId = clientRmOperations.getNewApplication().getApplicationId();
		log.info("submitApplication, got applicationId=[" + applicationId + "]");

		if (resourceLocalizer instanceof SmartResourceLocalizer) {
			SmartResourceLocalizer smartResourceLocalizer = (SmartResourceLocalizer)resourceLocalizer;
			smartResourceLocalizer.setStagingId(applicationId.toString());
			if (!distribute) {
				smartResourceLocalizer.resolve();
			} else {
				smartResourceLocalizer.distribute();
			}
		} else {
			log.warn("Resource localizer is not instance of SmartResourceLocalizer, thus we're unable to resolve and distrute manually");
		}

		ApplicationSubmissionContext submissionContext = getSubmissionContext(applicationId);

		if (log.isDebugEnabled()) {
			log.debug("Using ApplicationSubmissionContext=" + submissionContext);
		}

		clientRmOperations.submitApplication(submissionContext);
		return applicationId;
	}

	@Override
	public void installApplication() {
		if (resourceLocalizer instanceof SmartResourceLocalizer) {
			((SmartResourceLocalizer)resourceLocalizer).copy();
		} else {
			log.warn("Resource localizer is not instance of SmartResourceLocalizer, thus we're unable to ask copy operation");
		}
	}

	@Override
	public void killApplication(ApplicationId applicationId) {
		clientRmOperations.killApplication(applicationId);
	}

	@Override
	public List<ApplicationReport> listApplications() {
		return clientRmOperations.listApplications();
	}

	@Override
	public List<ApplicationReport> listApplications(String type) {
		Set<String> appTypes = new HashSet<String>();
		if (StringUtils.hasText(type)) {
			appTypes.add(type);
		}
		return clientRmOperations.listApplications(null, appTypes);
	}

	@Override
	public List<ApplicationReport> listRunningApplications(String type) {
		EnumSet<YarnApplicationState> appStates = EnumSet.of(YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
				YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING);
		Set<String> appTypes = new HashSet<String>();
		if (StringUtils.hasText(type)) {
			appTypes.add(type);
		}
		return clientRmOperations.listApplications(appStates, appTypes);
	}

	@Override
	public ApplicationReport getApplicationReport(ApplicationId applicationId) {
		return clientRmOperations.getApplicationReport(applicationId);
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
	 * Sets the type for submitted application.
	 *
	 * @param appType the new application type
	 */
	public void setAppType(String appType) {
		this.appType = appType;
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
	public void setMemory(long memory) {
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
	 * Sets the application label expression.
	 *
	 * @param labelExpression the new application label expression
	 */
	public void setLabelExpression(String labelExpression) {
		this.labelExpression = labelExpression;
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
	 * @param applicationId application id
	 * @return the submission context
	 */
	protected ApplicationSubmissionContext getSubmissionContext(ApplicationId applicationId) {
		ApplicationSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
		context.setApplicationId(applicationId);
		context.setApplicationName(appName);
		if (StringUtils.hasText(appType)) {
			context.setApplicationType(appType);
		}
		context.setAMContainerSpec(getMasterContainerLaunchContext());

		Resource capability = Records.newRecord(Resource.class);
		capability.setMemorySize(memory);
		ResourceCompat.setVirtualCores(capability, virtualcores);
		context.setResource(capability);

		Priority record = Records.newRecord(Priority.class);
		record.setPriority(priority);
		context.setPriority(record);
		context.setQueue(queue);
		if (StringUtils.hasText(labelExpression)) {
			context.setNodeLabelExpression(labelExpression);
		}
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

		try {
			if (UserGroupInformation.isSecurityEnabled()) {
				Credentials credentials = new Credentials();
				final FileSystem fs = FileSystem.get(configuration);
				Token<?>[] tokens = fs.addDelegationTokens(YarnUtils.getPrincipal(configuration), credentials);
				if (tokens != null) {
					for (Token<?> token : tokens) {
						log.info("Got delegation token for " + fs.getUri() + "; " + token);
					}
				}
				DataOutputBuffer dob = new DataOutputBuffer();
				credentials.writeTokenStorageToStream(dob);
				ByteBuffer containerToken  = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
				context.setTokens(containerToken);
			}
		} catch (IOException e) {
			log.error("Error setting tokens for appmaster launch context", e);
		}

		return context;
	}

}
