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
package org.springframework.yarn.boot;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

public abstract class MockUtils {

	public static ContainerStatus getMockContainerStatus(ContainerId containerId, ContainerState containerState, int exitStatus) {
		ContainerStatus status = mock(ContainerStatus.class);
		when(status.getContainerId()).thenReturn(containerId);
		when(status.getState()).thenReturn(containerState);
		when(status.getExitStatus()).thenReturn(exitStatus);
		return status;
	}

	public static ApplicationId getMockApplicationId(int appId) {
		ApplicationId applicationId = mock(ApplicationId.class);
		when(applicationId.getClusterTimestamp()).thenReturn(0L);
		when(applicationId.getId()).thenReturn(appId);
		return applicationId;
	}

	public static Container getMockContainer(ContainerId containerId, NodeId nodeId, Resource resource,
			Priority priority) {
		Container container = mock(Container.class);
		when(container.getId()).thenReturn(containerId);
		when(container.getNodeId()).thenReturn(nodeId);
		when(container.getResource()).thenReturn(resource);
		when(container.getPriority()).thenReturn(priority);
		return container;
	}

	public static ContainerId getMockContainerId(ApplicationAttemptId applicationAttemptId, long id) {
		ContainerId containerId = mock(ContainerId.class);
		doReturn(applicationAttemptId).when(containerId).getApplicationAttemptId();
		doReturn(id).when(containerId).getContainerId();
		doReturn(Long.toString(id)).when(containerId).toString();
		return containerId;
	}

	public static Resource getMockResource(long mem, int cores) {
		Resource resource = mock(Resource.class);
		when(resource.getMemorySize()).thenReturn(mem);
		when(resource.getVirtualCores()).thenReturn(cores);
		return resource;
	}

	public static NodeId getMockNodeId(String host, int port) {
		NodeId nodeId = mock(NodeId.class);
		doReturn(host).when(nodeId).getHost();
		doReturn(port).when(nodeId).getPort();
		return nodeId;
	}

	public static Priority getMockPriority(int priority) {
		Priority pri = mock(Priority.class);
		when(pri.getPriority()).thenReturn(priority);
		return pri;
	}

	public static ApplicationAttemptId getMockApplicationAttemptId(int appId, int attemptId) {
		ApplicationId applicationId = mock(ApplicationId.class);
		when(applicationId.getClusterTimestamp()).thenReturn(0L);
		when(applicationId.getId()).thenReturn(appId);
		ApplicationAttemptId applicationAttemptId = mock(ApplicationAttemptId.class);
		when(applicationAttemptId.getApplicationId()).thenReturn(applicationId);
		when(applicationAttemptId.getAttemptId()).thenReturn(attemptId);
		return applicationAttemptId;
	}

}
