/*
 * Copyright 2013-2017 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.springframework.context.ApplicationListener;
import org.springframework.util.StringUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.container.AbstractLauncher;
import org.springframework.yarn.am.container.ContainerRegisterInfo;
import org.springframework.yarn.am.container.ContainerShutdown;
import org.springframework.yarn.event.AbstractYarnEvent;
import org.springframework.yarn.event.ContainerAllocationEvent;
import org.springframework.yarn.event.ContainerCompletedEvent;
import org.springframework.yarn.event.ContainerLaunchRequestFailedEvent;
import org.springframework.yarn.event.ContainerLaunchedEvent;
import org.springframework.yarn.event.ContainerRegisterEvent;

/**
 * Base implementation of application master where life-cycle
 * is based on events rather than a static information existing
 * prior the start of an instance.
 * <p>
 * Life-cycle of this instance is not bound to information or
 * states existing prior the startup of an application master.
 * Containers can be requested and launched on demand and application
 * master is then responsible to know when it's time to bail out
 * and end the application.
 * <p>
 * Due to complex need of event communication, the actual event system
 * is abstracted order to plug different systems for a need of
 * various use cases.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractEventingAppmaster extends AbstractServicesAppmaster
		implements ApplicationListener<AbstractYarnEvent> {

	private static final Log log = LogFactory.getLog(AbstractEventingAppmaster.class);

	private final Map<ContainerId, Container> runningContainers = new HashMap<ContainerId, Container>();

	private final Map<Container, ContainerRegisterInfo> registeredContainers = new HashMap<Container, ContainerRegisterInfo>();

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		if (getLauncher() instanceof AbstractLauncher) {
			((AbstractLauncher) getLauncher()).addInterceptor(new AddTrackServiceContainerLaunchInterceptor(
					getAppmasterTrackService()));
		}
	}

	@Override
	public void onApplicationEvent(AbstractYarnEvent event) {
		if (event instanceof ContainerAllocationEvent) {
			onContainerAllocated(((ContainerAllocationEvent) event).getContainer());
		} else if (event instanceof ContainerLaunchedEvent) {
			Container container = ((ContainerLaunchedEvent) event).getContainer();
			runningContainers.put(container.getId(), container);
			onContainerLaunched(container);
		} else if (event instanceof ContainerLaunchRequestFailedEvent) {
			onContainerLaunchRequestFailed(((ContainerLaunchRequestFailedEvent) event).getContainer());
		} else if (event instanceof ContainerCompletedEvent) {
			ContainerStatus containerStatus = ((ContainerCompletedEvent) event).getContainerStatus();
			Container container = runningContainers.remove(containerStatus.getContainerId());
			if (container != null) {
				registeredContainers.remove(container);
			}
			onContainerCompleted(containerStatus);
		} else if (event instanceof ContainerRegisterEvent) {
			ContainerRegisterEvent e = (ContainerRegisterEvent) event;
			ContainerId containerId = ContainerId.fromString(e.getContainerId());
			Container container = runningContainers.get(containerId);
			if (container != null) {
				registeredContainers.put(container, new ContainerRegisterInfo(e.getTrackUrl()));
			} else {
				log.warn("Could not find matching running container=[" + container + "] for containerId=[" + containerId + "]");
			}
		}
	}

	/**
	 * Invoked when {@link ContainerAllocationEvent} is received as an
	 * application event. Wrapped {@link Container} is passed to a method.
	 *
	 * @param container the container
	 */
	protected void onContainerAllocated(Container container) {
		if (log.isDebugEnabled()) {
			log.debug("onContainerAllocated:" + container);
		}
	}

	/**
	 * Invoked when {@link ContainerLaunchedEvent} is received as an
	 * application event. Wrapped {@link Container} is passed to a method.
	 *
	 * @param container the container
	 */
	protected void onContainerLaunched(Container container) {
		if (log.isDebugEnabled()) {
			log.debug("onContainerLaunched:" + container);
		}
	}

	/**
	 * Invoked when {@link ContainerLaunchRequestFailedEvent} is received as an
	 * application event. Wrapped {@link Container} is passed to a method.
	 *
	 * @param container the container
	 */
	protected void onContainerLaunchRequestFailed(Container container) {
		if (log.isDebugEnabled()) {
			log.debug("onContainerLaunchRequestFailed:" + container);
		}
	}

	/**
	 * Invoked when {@link ContainerCompletedEvent} is received as an
	 * application event. Wrapped {@link ContainerStatus} is passed to a method.
	 *
	 * @param status the container status
	 */
	protected void onContainerCompleted(ContainerStatus status) {
		if (log.isDebugEnabled()) {
			log.debug("onContainerCompleted:" + status);
		}
	}

	@Override
	protected boolean shutdownContainers() {
		ContainerShutdown shutdowner = getContainerShutdown();
		if (shutdowner != null) {
			log.info("Using a ContainerShutdown registered in context [" + shutdowner + "]");
			shutdowner.shutdown(getRegisteredContainers());
		}
		else {
			log.info("Shutting down remaining containers: "
					+ StringUtils.collectionToCommaDelimitedString(runningContainers.values()));
			for (Container container : runningContainers.values()) {
				try {
					log.info("Shutting down container " + container);
					getCmTemplate(container).stopContainers();
				} catch (Exception e) {
					log.warn("Got error stopping container " + container);
				}
			}
		}
		return true;
	}

	protected Map<Container, ContainerRegisterInfo> getRegisteredContainers() {
		return registeredContainers;
	}

	/**
	 * Interceptor adding container env variable for appmaster track service.
	 */
	private class AddTrackServiceContainerLaunchInterceptor implements ContainerLauncherInterceptor {

		private final AppmasterTrackService appmasterTrackService;

		AddTrackServiceContainerLaunchInterceptor(AppmasterTrackService appmasterTrackService) {
			this.appmasterTrackService = appmasterTrackService;
		}

		@Override
		public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
			if (appmasterTrackService != null && StringUtils.hasText(appmasterTrackService.getTrackUrl())) {
				String address = appmasterTrackService.getTrackUrl();
				log.debug("Adding " + YarnSystemConstants.AMSERVICE_TRACKURL + "=" + address
						+ " to container launch context");
				Map<String, String> environment = new HashMap<String, String>(context.getEnvironment());
				environment.put(YarnSystemConstants.AMSERVICE_TRACKURL, address);
				context.setEnvironment(environment);
				return context;
			}
			return context;
		}

	}

}
