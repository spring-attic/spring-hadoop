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
package org.springframework.yarn.am;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.util.Assert;
import org.springframework.yarn.YarnSystemException;
import org.springframework.yarn.am.assign.ContainerAssign;
import org.springframework.yarn.am.assign.DefaultContainerAssign;
import org.springframework.yarn.am.container.ContainerShutdown;
import org.springframework.yarn.fs.ResourceLocalizer;
import org.springframework.yarn.fs.SmartResourceLocalizer;
import org.springframework.yarn.listener.AppmasterStateListener;
import org.springframework.yarn.listener.AppmasterStateListener.AppmasterState;
import org.springframework.yarn.listener.CompositeAppmasterStateListener;
import org.springframework.yarn.support.LifecycleObjectSupport;
import org.springframework.yarn.support.YarnContextUtils;
import org.springframework.yarn.support.YarnUtils;

/**
 * Base class providing functionality for common application
 * master instances.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractAppmaster extends LifecycleObjectSupport {

	private static final Log log = LogFactory.getLog(AbstractAppmaster.class);

	/** Environment variables for the process */
	private final HashMap<String, Map<String, String>> environments = new HashMap<String, Map<String,String>>();

	/** Yarn configuration */
	private Configuration configuration;

	/** Commands for container start where value mapped with null key indicates defaults */
	private final Map<String, List<String>> commands = new HashMap<String, List<String>>();

	/** Template operations talking to resource manager */
	private AppmasterRmOperations rmTemplate;

	/** Cached app attempt id */
	private ApplicationAttemptId applicationAttemptId;

	/** Parameters passed to application */
	private Properties parameters;

	/** Resource localizer for the containers */
	private ResourceLocalizer resourceLocalizer;

	/** Handle to service if exists */
	private AppmasterService appmasterService;

	/** Handle to client service if exists */
	private AppmasterService appmasterClientService;

	/** Handle to track service if exists */
	private AppmasterTrackService appmasterTrackService;

	/** Handle to container shutdown if exists */
	private ContainerShutdown containerShutdown;

	/** State if we're done successful registration */
	private boolean applicationRegistered;

	/** Appstatus when we send finish request */
	private FinalApplicationStatus finalApplicationStatus;

	/** Listener handling state events */
	private CompositeAppmasterStateListener stateListener = new CompositeAppmasterStateListener();

	/** Holder for container assigned data */
	private ContainerAssign<Object> containerAssign = new DefaultContainerAssign();

	/**
	 * Global application master instance specific {@link ApplicationAttemptId}
	 * is build during this init method.
	 *
	 * @see org.springframework.yarn.support.LifecycleObjectSupport#onInit()
	 */
	@Override
	protected void onInit() throws Exception {
		super.onInit();
		AppmasterRmTemplate armt = new AppmasterRmTemplate(getConfiguration());
		armt.afterPropertiesSet();
		rmTemplate = armt;
	}

	@Override
	protected void doStop() {
		finishAppmaster();
		// TODO: can we do this also here???
		//       we usually call this from subclasses
		//       but if context is shutdown this is our
		//       own hook to notify exit. See comments in
		//       BatchAppmaster.doStop()
		notifyCompleted();
	}

	/**
	 * Gets the {@link AppmasterRmOperations} template.
	 *
	 * @return the {@link AppmasterRmOperations} template
	 */
	public AppmasterRmOperations getTemplate() {
		return rmTemplate;
	}

	/**
	 * Sets the {@link AppmasterRmOperations} template.
	 *
	 * @param template the new {@link AppmasterRmOperations} template
	 */
	public void setTemplate(AppmasterRmOperations template) {
		this.rmTemplate = template;
	}

	/**
	 * Gets the environment variables.
	 *
	 * @return the environment variables
	 */
	public Map<String, String> getEnvironment() {
		return getEnvironment(null);
	}

	public Map<String, String> getEnvironment(String id) {
		return environments.get(id);
	}

	/**
	 * Sets the environment variables.
	 *
	 * @param environment the environment variables
	 */
	public void setEnvironment(Map<String, String> environment) {
		setEnvironment(null, environment);
	}

	public void setEnvironment(String id, Map<String, String> environment) {
		environments.put(id, environment);
	}

	/**
	 * Gets the parameters.
	 *
	 * @return the parameters
	 */
	public Properties getParameters() {
		return parameters;
	}

	/**
	 * Sets the parameters.
	 *
	 * @param parameters the new parameters
	 */
	public void setParameters(Properties parameters) {
		this.parameters = parameters;
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
	 * Gets the commands.
	 *
	 * @return the commands
	 */
	public List<String> getCommands() {
		return commands.get(null);
	}

	/**
	 * Gets the commands.
	 *
	 * @param id the commands identifier
	 * @return the commands
	 */
	public List<String> getCommands(String id) {
		return commands.get(id);
	}

	/**
	 * Sets the commands.
	 *
	 * @param commands the new commands
	 */
	public void setCommands(List<String> commands) {
		this.commands.put(null, commands);
	}

	/**
	 * Sets the commands with an identifier.
	 *
	 * @param id the commands identifier
	 * @param commands the new commands
	 */
	public void setCommands(String id, List<String> commands) {
		this.commands.put(id, commands);
	}

	/**
	 * Sets the commands.
	 *
	 * @param commands the new commands
	 */
	public void setCommands(String[] commands) {
		setCommands(Arrays.asList(commands));
	}

	/**
	 * Sets the commands with an identifier.
	 *
	 * @param id the commands identifier
	 * @param commands the new commands
	 */
	public void setCommands(String id, String[] commands) {
		setCommands(id, Arrays.asList(commands));
	}

	/**
	 * Gets the application attempt id.
	 *
	 * @return the application attempt id
	 */
	protected ApplicationAttemptId getApplicationAttemptId() {
		return applicationAttemptId;
	}

	/**
	 * Sets the resource localizer.
	 *
	 * @param resourceLocalizer the new resource localizer
	 */
	public void setResourceLocalizer(ResourceLocalizer resourceLocalizer) {
		this.resourceLocalizer = resourceLocalizer;
	}

	/**
	 * Gets the resource localizer.
	 *
	 * @return the resource localizer
	 */
	public ResourceLocalizer getResourceLocalizer() {
		return resourceLocalizer;
	}

	/**
	 * Adds the appmaster state listener.
	 *
	 * @param listener the listener
	 */
	public void addAppmasterStateListener(AppmasterStateListener listener) {
		stateListener.register(listener);
	}

	/**
	 * Gets the container assign.
	 *
	 * @return the container assign
	 */
	public ContainerAssign<Object> getContainerAssign() {
		return containerAssign;
	}

	/**
	 * Sets the container assign.
	 *
	 * @param containerAssign the new container assign
	 */
	public void setContainerAssign(ContainerAssign<Object> containerAssign) {
		this.containerAssign = containerAssign;
	}

	/**
	 * Sets the final application status.
	 *
	 * @param finalApplicationStatus the new final application status
	 */
	protected void setFinalApplicationStatus(FinalApplicationStatus finalApplicationStatus) {
		this.finalApplicationStatus = finalApplicationStatus;
	}

	/**
	 * Notify completed state to appmaster state listeners.
	 */
	protected void notifyCompleted() {
		stateListener.state(AppmasterState.COMPLETED);
	}

	/**
	 * Gets a {@link AppmasterService} set to this instance.
	 *
	 * @return the instance of {@link AppmasterService}
	 */
	protected AppmasterService getAppmasterService() {
		if(appmasterService == null && getBeanFactory() != null) {
			log.debug("getting appmaster service from bean factory " + getBeanFactory());
			appmasterService = YarnContextUtils.getAppmasterService(getBeanFactory());
		}
		return appmasterService;
	}

	/**
	 * Gets a client facing {@link AppmasterService} set to this instance.
	 *
	 * @return the instance of {@link AppmasterService}
	 */
	protected AppmasterService getAppmasterClientService() {
		if(appmasterClientService == null && getBeanFactory() != null) {
			log.debug("getting appmaster client service from bean factory " + getBeanFactory());
			appmasterClientService = YarnContextUtils.getAppmasterClientService(getBeanFactory());
		}
		return appmasterClientService;
	}

	/**
	 * Gets a {@link ContainerShutdown} set to this instance.
	 *
	 * @return the instance of {@link ContainerShutdown}
	 */
	protected ContainerShutdown getContainerShutdown() {
		if (containerShutdown == null && getBeanFactory() != null) {
			log.debug("getting container shutdown from bean factory " + getBeanFactory());
			containerShutdown = YarnContextUtils.getContainerShutdown(getBeanFactory());
		}
		return containerShutdown;
	}

	/**
	 * Gets a {@link AppmasterTrackService} set to this instance.
	 *
	 * @return the instance of {@link AppmasterTrackService}
	 */
	protected AppmasterTrackService getAppmasterTrackService() {
		if(appmasterTrackService == null && getBeanFactory() != null) {
			log.debug("getting appmaster track service from bean factory " + getBeanFactory());
			appmasterTrackService = YarnContextUtils.getAppmasterTrackService(getBeanFactory());
		}
		return appmasterTrackService;
	}

	/**
	 * Register appmaster.
	 *
	 * @return the register application master response
	 */
	protected RegisterApplicationMasterResponse registerAppmaster() {
		applicationAttemptId = YarnUtils.getApplicationAttemptId(getEnvironment());
		Assert.notNull(applicationAttemptId, "applicationAttemptId must be set");
		if(applicationRegistered) {
			log.warn("Not sending register request because we are already registered");
			return null;
		}

		// resolving tracking url if any
		String trackUrl = getAppmasterTrackService() != null ? getAppmasterTrackService().getTrackUrl() : null;

		// resolving client facing service if any
		String rpcHost = null;
		Integer rpcPort = null;
		AppmasterService clientService = getAppmasterClientService();
		if (clientService != null) {
			rpcHost = clientService.getHost();
			rpcPort = clientService.getPort();
		}

		log.info("Registering application master with applicationAttemptId=" + applicationAttemptId +
				" trackUrl=" + trackUrl + " rpcHost=" + rpcHost + " rpcPort=" + rpcPort);

		RegisterApplicationMasterResponse response =
				rmTemplate.registerApplicationMaster(rpcHost, rpcPort, trackUrl);
		applicationRegistered = true;
		return response;
	}

	/**
	 * Finish appmaster by sending request to resource manager. Default
	 * application status is {@code FinalApplicationStatus.SUCCEEDED} which
	 * can be changed using method {@link #setFinalApplicationStatus(FinalApplicationStatus)}.
	 *
	 * @return the finish application master response
	 */
	protected FinishApplicationMasterResponse finishAppmaster() {

		boolean cleaned = false;
		if (resourceLocalizer instanceof SmartResourceLocalizer) {
			cleaned = ((SmartResourceLocalizer)resourceLocalizer).clean();
		} else {
			log.warn("Resource localizer is not instance of SmartResourceLocalizer, thus we're not asking cleanup");
		}

		log.info("Status of resource localizer clean operation is " + cleaned);

		// starting from 2.1.x applicationAttemptId is part of the token and
		// doesn't exist in finish request. We still keep it around as per
		// old concept.
		Assert.notNull(applicationAttemptId, "applicationAttemptId must be set");
		if(!applicationRegistered) {
			log.warn("Not sending finish request because we're not registered");
			return null;
		}

		shutdownContainers();

		FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
		// assume succeed if not set
		FinalApplicationStatus status = finalApplicationStatus != null ?
				finalApplicationStatus : FinalApplicationStatus.SUCCEEDED;

		if(log.isDebugEnabled()) {
			log.debug("Sending finish request to resource manager. Current applicationAttemptId=" +
					applicationAttemptId + " with status=" + status);
		}

		finishReq.setFinalApplicationStatus(status);
		return rmTemplate.finish(finishReq);
	}

	/**
	 * Creates an {@link AppmasterCmOperations} template.
	 *
	 * @param container the container
	 * @return the container manager operations template
	 */
	protected AppmasterCmOperations getCmTemplate(Container container) {
		try {
			AppmasterCmTemplate template = new AppmasterCmTemplate(getConfiguration(), container);
			template.afterPropertiesSet();
			return template;
		} catch (Exception e) {
			throw new YarnSystemException("Unable to create AppmasterCmTemplate", e);
		}
	}

	/**
	 * Shutdown containers. This method is automatically called before appmaster
	 * is sending finish request to a resource manager. Sub-classes should do
	 * their shutdown actions. This default implementation doesn't do anything.
	 *
	 * @return true, if container shutdown is considered successful
	 */
	protected boolean shutdownContainers() {
		return true;
	}

}
