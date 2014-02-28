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
	 * Gets a count of free containers.
	 *
	 * @return count of free containers
	 */
	int freeCount();

	/**
	 * Gets a count of running containers.
	 *
	 * @return count of running containers
	 */
	int runningCount();

	/**
	 * Gets a count of failed containers.
	 *
	 * @return count of failed containers
	 */
	int failedCount();

	/**
	 * Gets a count of completed containers.
	 *
	 * @return count of completed containers
	 */
	int completedCount();

	/**
	 * Adds the container monitor state listener.
	 *
	 * @param listener the {@link ContainerMonitorListener}
	 */
	void addContainerMonitorStateListener(ContainerMonitorListener listener);

}
