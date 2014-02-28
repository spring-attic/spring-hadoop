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
package org.springframework.yarn.event;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;

/**
 * The default strategy for publishing Yarn events.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultYarnEventPublisher implements YarnEventPublisher, ApplicationEventPublisherAware {

	/** The one who can publish events from this implementation */
	private ApplicationEventPublisher applicationEventPublisher;

	/**
	 * Constructs default strategy without an event publisher.
	 */
	public DefaultYarnEventPublisher() {
		this(null);
	}

	/**
	 * Constructs default strategy with an event publisher.
	 *
	 * @param applicationEventPublisher the event publisher
	 */
	public DefaultYarnEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		super();
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Override
	public void publishContainerAllocated(Object source, Container container) {
		if (applicationEventPublisher != null) {
			applicationEventPublisher.publishEvent(new ContainerAllocationEvent(source, container));
		}
	}

	@Override
	public void publishContainerLaunched(Object source, Container container) {
		if (applicationEventPublisher != null) {
			applicationEventPublisher.publishEvent(new ContainerLaunchedEvent(source, container));
		}
	}

	@Override
	public void publishContainerLaunchRequestFailed(Object source, Container container) {
		if (applicationEventPublisher != null) {
			applicationEventPublisher.publishEvent(new ContainerLaunchRequestFailedEvent(source, container));
		}
	}

	@Override
	public void publishContainerCompleted(Object source, ContainerStatus status) {
		if (applicationEventPublisher != null) {
			applicationEventPublisher.publishEvent(new ContainerCompletedEvent(source, status));
		}
	}

	@Override
	public void publishEvent(AbstractYarnEvent event) {
		if (applicationEventPublisher != null) {
			applicationEventPublisher.publishEvent(event);
		}
	}

}
