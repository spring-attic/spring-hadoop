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
package org.springframework.yarn.listener;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

/**
 * Interface used for allocator to notify newly allocated
 * and completed containers.
 *
 * @author Janne Valkealahti
 *
 */
public interface ContainerAllocatorListener {

	/**
	 * Invoked when new containers are allocated.
	 *
	 * @param allocatedContainers list of allocated {@link Container}s
	 */
	void allocated(List<Container> allocatedContainers);

	/**
	 * Invoked when containers are releases and thus
	 * marked as completed.
	 *
	 * @param completedContainers list of completed {@link ContainerStatus}s
	 */
	void completed(List<ContainerStatus> completedContainers);

}
