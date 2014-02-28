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
package org.springframework.yarn.event;

import org.apache.hadoop.yarn.api.records.Container;

/**
 * Generic event representing that {@link Container} launch request has failed.
 * This event is not an indication that container somewhat failed what is
 * was supposed to do, instead failure indicates a failed request to
 * start a container.
 *
 * @author Janne Valkealahti
 *
 */
@SuppressWarnings("serial")
public class ContainerLaunchRequestFailedEvent extends AbstractYarnEvent {

	/** The container on which the Event initially occurred. */
	private Container container;

	/**
	 * Constructs event with the given {@link Container}.
	 *
	 * @param source the component that published the event (never {@code null})
	 * @param container the Container
	 */
	public ContainerLaunchRequestFailedEvent(Object source, Container container) {
		super(source);
		this.container = container;
	}

	/**
	 * Gets the container.
	 *
	 * @return the container
	 */
	public Container getContainer() {
		return container;
	}

	@Override
	public String toString() {
		return "ContainerLaunchRequestFailedEvent [containerId=" + container.getId()
				+ ", nodeId=" + container.getNodeId()
				+ "]";
	}

}
