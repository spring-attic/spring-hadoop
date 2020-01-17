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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Test;
import org.springframework.yarn.TestUtils;

public class DefaultContainerMonitorTests {

	@Test
	public void testInitialStatus() throws Exception {
		DefaultContainerMonitor monitor = new DefaultContainerMonitor();
		assertMonitorState(monitor, 0, 0, 0, 0);
	}

	@Test
	public void testSucceed() throws Exception {
		DefaultContainerMonitor monitor = new DefaultContainerMonitor();

		ApplicationAttemptId applicationAttemptId = getMockApplicationAttemptId(1, 1);

		ContainerId containerId1 = getMockContainerId(applicationAttemptId, 1);
		Container container1 = getMockContainer(containerId1, null, null, null);

		// container 1
		// failed free running
		// allocated running completed failed
		monitor.onContainer(Arrays.asList(container1));
		assertMonitorState(monitor, 1, 0, 0, 0);

		monitor.onContainerStatus(Arrays.asList(getMockContainerStatus(containerId1, ContainerState.COMPLETE, 0)));
		assertMonitorState(monitor, 0, 0, 1, 0);
	}

	@Test
	public void testTwoSucceed() throws Exception {
		DefaultContainerMonitor monitor = new DefaultContainerMonitor();

		ApplicationAttemptId applicationAttemptId = getMockApplicationAttemptId(1, 1);

		ContainerId containerId1 = getMockContainerId(applicationAttemptId, 1);
		Container container1 = getMockContainer(containerId1, null, null, null);
		ContainerId containerId2 = getMockContainerId(applicationAttemptId, 2);
		Container container2 = getMockContainer(containerId2, null, null, null);

		// container 1
		// failed free running
		// allocated running completed failed
		monitor.onContainer(Arrays.asList(container1));
		assertMonitorState(monitor, 1, 0, 0, 0);

		monitor.onContainerStatus(Arrays.asList(getMockContainerStatus(containerId1, ContainerState.COMPLETE, 0)));
		assertMonitorState(monitor, 0, 0, 1, 0);

		monitor.onContainer(Arrays.asList(container2));
		assertMonitorState(monitor, 1, 0, 1, 0);

		monitor.onContainerStatus(Arrays.asList(getMockContainerStatus(containerId2, ContainerState.COMPLETE, 0)));
		assertMonitorState(monitor, 0, 0, 2, 0);
	}

	@Test
	public void testFailedAndSucceedWithGarbage() throws Exception {
		DefaultContainerMonitor monitor = new DefaultContainerMonitor();

		ApplicationAttemptId applicationAttemptId = getMockApplicationAttemptId(1, 1);

		ContainerId containerId1 = getMockContainerId(applicationAttemptId, 1);
		Container container1 = getMockContainer(containerId1, null, null, null);
		ContainerId containerId2 = getMockContainerId(applicationAttemptId, 2);
		Container container2 = getMockContainer(containerId2, null, null, null);
		ContainerId containerId3 = getMockContainerId(applicationAttemptId, 3);
		//Container container3 = getMockContainer(containerId3, null, null, null, ContainerState.NEW);

		// container 1
		// failed free running
		// allocated running completed failed
		monitor.onContainer(Arrays.asList(container1));
		assertMonitorState(monitor, 1, 0, 0, 0);

		monitor.onContainerStatus(Arrays.asList(getMockContainerStatus(containerId1, ContainerState.COMPLETE, 0)));
		assertMonitorState(monitor, 0, 0, 1, 0);

		// container 2
		monitor.onContainer(Arrays.asList(container2));
		assertMonitorState(monitor, 1, 0, 1, 0);

		monitor.onContainerStatus(Arrays.asList(getMockContainerStatus(containerId2, ContainerState.COMPLETE, 0)));
		assertMonitorState(monitor, 0, 0, 2, 0);

		// container 3
		monitor.onContainerStatus(Arrays.asList(getMockContainerStatus(containerId3, ContainerState.COMPLETE, -100)));
		assertMonitorState(monitor, 0, 0, 2, 1);

	}

	@Test
	public void testFailedWithContainerLoss() throws Exception {
		// SHDP-385 case when nm is killed and eventually rm receives
		// Container released on a *lost* node" exit_status: -100
		DefaultContainerMonitor monitor = new DefaultContainerMonitor();
		ApplicationAttemptId applicationAttemptId = getMockApplicationAttemptId(1, 1);
		ContainerId containerId1 = getMockContainerId(applicationAttemptId, 1);
		Container container1 = getMockContainer(containerId1, null, null, null);
		monitor.onContainer(Arrays.asList(container1));
		assertMonitorState(monitor, 1, 0, 0, 0);

		monitor.onContainerStatus(Arrays.asList(getMockContainerStatus(containerId1, ContainerState.COMPLETE, -100)));
		assertMonitorState(monitor, 0, 0, 0, 1);
	}

	@Test
	public void testFailedWithContainerDiskFailed() throws Exception {
		DefaultContainerMonitor monitor = new DefaultContainerMonitor();
		ApplicationAttemptId applicationAttemptId = getMockApplicationAttemptId(1, 1);
		ContainerId containerId1 = getMockContainerId(applicationAttemptId, 1);
		Container container1 = getMockContainer(containerId1, null, null, null);
		monitor.onContainer(Arrays.asList(container1));
		assertMonitorState(monitor, 1, 0, 0, 0);

		monitor.onContainerStatus(Arrays.asList(getMockContainerStatus(containerId1, ContainerState.COMPLETE, -101)));
		assertMonitorState(monitor, 0, 0, 0, 1);
	}

	@Test
	public void testFailedWithContainerInvalid() throws Exception {
		DefaultContainerMonitor monitor = new DefaultContainerMonitor();
		ApplicationAttemptId applicationAttemptId = getMockApplicationAttemptId(1, 1);
		ContainerId containerId1 = getMockContainerId(applicationAttemptId, 1);
		Container container1 = getMockContainer(containerId1, null, null, null);
		monitor.onContainer(Arrays.asList(container1));
		assertMonitorState(monitor, 1, 0, 0, 0);

		monitor.onContainerStatus(Arrays.asList(getMockContainerStatus(containerId1, ContainerState.COMPLETE, -1000)));
		assertMonitorState(monitor, 0, 0, 0, 1);
	}

	/**
	 * Assert states or allocated, running, completed and failed sets.
	 *
	 * @param monitor
	 * @param allocated
	 * @param running
	 * @param completed
	 * @param failed
	 * @throws Exception
	 */
	private void assertMonitorState(DefaultContainerMonitor monitor, int allocated, int running, int completed, int failed) throws Exception {
		Set<ContainerId> allocatedSet = TestUtils.readField("allocated", monitor);
		Set<ContainerId> runningSet = TestUtils.readField("running", monitor);
		Set<ContainerId> completedSet = TestUtils.readField("completed", monitor);
		Set<ContainerId> failedSet = TestUtils.readField("failed", monitor);
		assertThat(allocatedSet.size(), is(allocated));
		assertThat(runningSet.size(), is(running));
		assertThat(completedSet.size(), is(completed));
		assertThat(failedSet.size(), is(failed));
	}

	/**
	 * Mock {@link ContainerStatus}
	 *
	 * @param containerId the {@link ContainerId}
	 * @param containerState the {@link ContainerState}
	 * @param exitStatus the container exit status
	 * @return mocked {@link ContainerStatus}
	 */
	public static ContainerStatus getMockContainerStatus(ContainerId containerId, ContainerState containerState, int exitStatus) {
		ContainerStatus status = mock(ContainerStatus.class);
		when(status.getContainerId()).thenReturn(containerId);
		when(status.getState()).thenReturn(containerState);
		when(status.getExitStatus()).thenReturn(exitStatus);
		return status;
	}

	/**
	 * Mock {@link Container}
	 *
	 * @param containerId the {@link ContainerId}
	 * @param nodeId the {@link NodeId}
	 * @param resource the {@link Resource}
	 * @param priority the {@link Priority}
	 * @param state the {@link ContainerState}
	 * @return mocked {@link Container}
	 */
	public static Container getMockContainer(ContainerId containerId, NodeId nodeId, Resource resource,
			Priority priority) {
		Container container = mock(Container.class);
		when(container.getId()).thenReturn(containerId);
		when(container.getNodeId()).thenReturn(nodeId);
		when(container.getResource()).thenReturn(resource);
		when(container.getPriority()).thenReturn(priority);
		return container;
	}

	/**
	 * Mock {@link ApplicationId}
	 *
	 * @param appId the app id
	 * @return mocked {@link ApplicationId}
	 */
	public static ApplicationId getMockApplicationId(int appId) {
		ApplicationId applicationId = mock(ApplicationId.class);
		when(applicationId.getClusterTimestamp()).thenReturn(0L);
		when(applicationId.getId()).thenReturn(appId);
		return applicationId;
	}

	/**
	 * Mock {@link ContainerId}
	 *
	 * @param applicationAttemptId the application attempt id
	 * @param id the container id
	 * @return mocked {@link ContainerId}
	 */
	public static ContainerId getMockContainerId(ApplicationAttemptId applicationAttemptId, long id) {
		ContainerId containerId = mock(ContainerId.class);
		doReturn(applicationAttemptId).when(containerId).getApplicationAttemptId();
		doReturn(id).when(containerId).getContainerId();
		doReturn(Long.toString(id)).when(containerId).toString();
		return containerId;
	}

	/**
	 * Mock {@link ApplicationAttemptId}
	 *
	 * @param appId the app id
	 * @param attemptId the app attempt id
	 * @return mocked {@link ApplicationAttemptId}
	 */
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
