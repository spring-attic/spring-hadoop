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

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

/**
 * Composite listener for handling allocation events.
 *
 * @author Janne Valkealahti
 *
 */
public class CompositeContainerAllocatorListener extends AbstractCompositeListener<ContainerAllocatorListener>
		implements ContainerAllocatorListener {

	@Override
	public void allocated(List<Container> allocatedContainers) {
		for (Iterator<ContainerAllocatorListener> iterator = getListeners().reverse(); iterator.hasNext();) {
			ContainerAllocatorListener listener = iterator.next();
			listener.allocated(allocatedContainers);
		}
	}

	@Override
	public void completed(List<ContainerStatus> completedContainers) {
		for (Iterator<ContainerAllocatorListener> iterator = getListeners().reverse(); iterator.hasNext();) {
			ContainerAllocatorListener listener = iterator.next();
			listener.completed(completedContainers);
		}
	}

}
