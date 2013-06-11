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
package org.springframework.yarn.am.monitor;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.springframework.yarn.listener.ContainerMonitorListener;

/**
 * General interface for components able to monitor application
 * and container statuses.
 *
 * @author Janne Valkealahti
 *
 */
public interface ContainerMonitor {

	/**
	 * Adds a container into monitor.
	 *
	 * @param container the container
	 */
	void addContainer(Container container);

	/**
	 * Notifies monitor for completed containers.
	 *
	 * @param completedContainers the completed containers
	 */
	void monitorContainer(List<ContainerStatus> completedContainers);

	/**
	 * Notifies monitor for completed container.
	 *
	 * @param completedContainer the completed container
	 */
	void monitorContainer(ContainerStatus completedContainer);

	/**
	 * Report container.
	 *
	 * @param container the container
	 */
	void reportContainer(Container container);

	/**
	 * Checks for running containers.
	 *
	 * @return true, if running containers exist
	 */
	boolean hasRunning();

	/**
	 * Checks for failed containers.
	 *
	 * @return true, if failed containers exist
	 */
	boolean hasFailed();

	/**
	 * Checks for free allocated containers.
	 *
	 * @return true, if free allocated containers exist
	 */
	boolean hasFree();

	/**
	 * Adds the container monitor state listener.
	 *
	 * @param listener the {@link ContainerMonitorListener}
	 */
	void addContainerMonitorStateListener(ContainerMonitorListener listener);

}
