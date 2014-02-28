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

/**
 * Interface for publishing Yarn based application events.
 *
 * @author Janne Valkealahti
 *
 */
public interface YarnEventPublisher {

	/**
	 * Publish an application event containing information
	 * about the allocated {@link Container}.
	 *
	 * @param source the component that published the event (never {@code null})
	 * @param container the Container
	 */
	void publishContainerAllocated(Object source, Container container);

	/**
	 * Publish an application event containing information
	 * about the launched {@link Container}.
	 *
	 * @param source the component that published the event (never {@code null})
	 * @param container the Container
	 */
	void publishContainerLaunched(Object source, Container container);

	/**
	 * Publish an application event containing information
	 * about the failed {@link Container} launch request.
	 *
	 * @param source the component that published the event (never {@code null})
	 * @param container the Container
	 */
	void publishContainerLaunchRequestFailed(Object source, Container container);

	/**
	 * Publish an application event containing information
	 * about the completed {@link ContainerStatus}.
	 *
	 * @param source the component that published the event (never {@code null})
	 * @param status the Container status
	 */
	void publishContainerCompleted(Object source, ContainerStatus status);

	/**
	 * Publish a general application event of type {@link AbstractYarnEvent}.
	 *
	 * @param event the event
	 */
	void publishEvent(AbstractYarnEvent event);

}
