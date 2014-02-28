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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.springframework.context.ApplicationListener;
import org.springframework.yarn.event.AbstractYarnEvent;
import org.springframework.yarn.event.ContainerAllocationEvent;
import org.springframework.yarn.event.ContainerCompletedEvent;
import org.springframework.yarn.event.ContainerLaunchRequestFailedEvent;
import org.springframework.yarn.event.ContainerLaunchedEvent;

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

	@Override
	public void onApplicationEvent(AbstractYarnEvent event) {
		if (event instanceof ContainerAllocationEvent) {
			onContainerAllocated(((ContainerAllocationEvent) event).getContainer());
		} else if (event instanceof ContainerLaunchedEvent) {
			onContainerLaunched(((ContainerLaunchedEvent) event).getContainer());
		} else if (event instanceof ContainerLaunchRequestFailedEvent) {
			onContainerLaunchRequestFailed(((ContainerLaunchRequestFailedEvent) event).getContainer());
		} else if (event instanceof ContainerCompletedEvent) {
			onContainerCompleted(((ContainerCompletedEvent) event).getContainerStatus());
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

}
