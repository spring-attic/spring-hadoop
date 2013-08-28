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
package org.springframework.yarn.am.assign;

import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * Interface storing and keeping data associated with
 * a container. This is useful when i.e. container is
 * failing and appmaster needs to decide what to do
 * with it.
 *
 * @author Janne Valkealahti
 *
 * @param <E> the type of data assigned
 *
 */
public interface ContainerAssign<E> {

	/**
	 * Assign data with container.
	 *
	 * @param containerId the container id
	 * @param data the data
	 */
	void assign(ContainerId containerId, E data);

	/**
	 * Gets the assigned data.
	 *
	 * @param containerId the container id
	 * @return the assigned data
	 */
	E getAssignedData(ContainerId containerId);

	/**
	 * Gets the assigned container.
	 *
	 * @param data the data
	 * @return the assigned container
	 */
	ContainerId getAssignedContainer(E data);

}
