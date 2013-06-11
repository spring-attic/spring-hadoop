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
package org.springframework.yarn.am;

import java.util.Arrays;
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
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.util.Assert;
import org.springframework.yarn.am.assign.ContainerAssing;
import org.springframework.yarn.am.assign.DefaultContainerAssing;
import org.springframework.yarn.fs.ResourceLocalizer;
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
	private Map<String, String> environment;

	/** Yarn configuration */
	private Configuration configuration;

	/** Commands for container start */
	private List<String> commands;

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

	/** State if we're done successful registration */
	private boolean applicationRegistered;

	/** Appstatus when we send finish request */
	private FinalApplicationStatus finalApplicationStatus;

	/** Listener handling state events */
	private CompositeAppmasterStateListener stateListener = new CompositeAppmasterStateListener();

	/** Holder for container assigned data */
	private ContainerAssing<Object> containerAssing = new DefaultContainerAssing();

	/**
	 * Global application master instance specific {@link ApplicationAttemptId}
	 * is build during this init method.
	 *
	 * @see org.springframework.yarn.support.LifecycleObjectSupport#onInit()
	 */
	@Override
	protected void onInit() throws Exception {
		super.onInit();
		applicationAttemptId = YarnUtils.getApplicationAttemptId(environment);
		AppmasterRmTemplate armt = new AppmasterRmTemplate(getConfiguration());
		armt.afterPropertiesSet();
		rmTemplate = armt;
	}

	@Override
	protected void doStop() {
		finishAppmaster();
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
		return environment;
	}

	/**
	 * Sets the environment variables.
	 *
	 * @param environment the environment variables
	 */
	public void setEnvironment(Map<String, String> environment) {
		this.environment = environment;
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
		return commands;
	}

	/**
	 * Sets the commands.
	 *
	 * @param commands the new commands
	 */
	public void setCommands(List<String> commands) {
		this.commands = commands;
	}

	/**
	 * Sets the commands.
	 *
	 * @param commands the new commands
	 */
	public void setCommands(String[] commands) {
		this.commands = Arrays.asList(commands);
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
	 * Gets the container assing.
	 *
	 * @return the container assing
	 */
	public ContainerAssing<Object> getContainerAssing() {
		return containerAssing;
	}

	/**
	 * Sets the container assing.
	 *
	 * @param containerAssing the new container assing
	 */
	public void setContainerAssing(ContainerAssing<Object> containerAssing) {
		this.containerAssing = containerAssing;
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
	 * Register appmaster.
	 *
	 * @return the register application master response
	 */
	protected RegisterApplicationMasterResponse registerAppmaster() {
		Assert.notNull(applicationAttemptId, "applicationAttemptId must be set");
		if(applicationRegistered) {
			log.warn("Not sending register request because we are already registered");
			return null;
		}
		log.info("Registering application master with applicationAttemptId=" + applicationAttemptId);
		RegisterApplicationMasterResponse response =
				rmTemplate.registerApplicationMaster(applicationAttemptId, null, null, null);
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

		boolean clean = getResourceLocalizer().clean();
		log.info("Status of resource localizer clean operation is " + clean);

		Assert.notNull(applicationAttemptId, "applicationAttemptId must be set");
		if(!applicationRegistered) {
			log.warn("Not sending finish request because we're not registered");
			return null;
		}
		if(log.isDebugEnabled()) {
			log.debug("Sending finish request to resource manager: appAttemptId=" +
					applicationAttemptId + " status=" + FinalApplicationStatus.SUCCEEDED);
		}
		FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
		finishReq.setAppAttemptId(applicationAttemptId);

		// assume succeed if not set
		FinalApplicationStatus status = finalApplicationStatus != null ?
				finalApplicationStatus : FinalApplicationStatus.SUCCEEDED;
		finishReq.setFinishApplicationStatus(status);
		return rmTemplate.finish(finishReq);
	}

}
