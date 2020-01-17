/*
 * Copyright 2014-2015 the original author or authors.
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
package org.springframework.yarn.am.cluster;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.junit.Test;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.listener.StateMachineListenerAdapter;
import org.springframework.statemachine.state.State;
import org.springframework.yarn.TestUtils;
import org.springframework.yarn.am.grid.GridProjection;
import org.springframework.yarn.am.grid.support.ProjectionData;
import org.springframework.yarn.am.grid.support.SatisfyStateData;

/**
 * Tests for {@link ManagedContainerClusterAppmaster} using single cluster.
 *
 * @author Janne Valkealahti
 *
 */
public class ManagedContainerClusterAppmasterSingleTests extends AbstractManagedContainerClusterAppmasterTests {

	@Test
	public void testInitialState() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);
		assertDoTask(appmaster, 0, 0, 0, 0, "foo", true);
	}

	@Test
	public void testCreateOneCluster() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);
		ProjectionData projectionData = new ProjectionData(1, null, null, "default", null);
		appmaster.createContainerCluster("cluster", projectionData);
		assertDoTask(appmaster, 0, 0, 0, 0, "foo", true);
	}

	@Test
	public void testCreateStartOneCluster() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);
		ProjectionData projectionData = new ProjectionData(1, null, null, "default", null);
		appmaster.createContainerCluster("foo", projectionData);
		appmaster.startContainerCluster("foo");
		assertDoTask(appmaster, 1, 0, 0, 0, "foo");
	}

	@Test
	public void testCreateStartOneClusterDoAllocation() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);

		ProjectionData projectionData = new ProjectionData(1, null, null, "default", 0);
		projectionData.setMemory(0L);
		projectionData.setVirtualCores(0);

		appmaster.createContainerCluster("foo", projectionData);
		appmaster.startContainerCluster("foo");

		// should get 1 any alloc and no container kills
		TestUtils.callMethod("doTask", appmaster);

		assertThat(appmaster.getSatisfyStateDataByCluster("foo").getAllocateData(), notNullValue());
		assertThat(appmaster.getSatisfyStateDataByCluster("foo").getAllocateData().getAny(), is(1));
		assertThat(appmaster.getSatisfyStateDataByCluster("foo").getRemoveData(), notNullValue());

		// this is garbage, track dirty
		appmaster.resetTestData();
		TestUtils.callMethod("doTask", appmaster);
		assertThat(appmaster.getSatisfyStateDataByCluster("foo"), nullValue());

		allocateContainer(appmaster, 1);

		assertThat(launcher.container, notNullValue());
		assertThat(allocator.containerAllocateData, nullValue());
		assertThat(allocator.releaseContainers, nullValue());
	}

	@Test
	public void testCreateStartStopOneCluster() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);
		ProjectionData projectionData = new ProjectionData(1, null, null, "default", 0);
		projectionData.setMemory(0L);
		projectionData.setVirtualCores(0);

		// create
		GridProjection projection = appmaster.createContainerCluster("foo", projectionData).getGridProjection();
		assertDoTask(appmaster, null, null, null, null, "foo");

		// start
		appmaster.startContainerCluster("foo");
		assertDoTask(appmaster, 1, 0, 0, 0, "foo");

		// allocate container 1
		allocateContainer(appmaster, 1);
		assertThat(projection.getMembers().size(), is(1));
		assertDoTask(appmaster, null, null, null, null, "foo");

		// stop
		appmaster.resetTestData();
		appmaster.stopContainerCluster("foo");
		assertDoTask(appmaster, null, null, null, 1, "foo");
	}

	@Test
	public void testCreateStartModifyOneCluster() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);
		ProjectionData projectionData = new ProjectionData(1, null, null, "default", 0);
		projectionData.setMemory(0L);
		projectionData.setVirtualCores(0);

		// create
		GridProjection projection = appmaster.createContainerCluster("foo", projectionData).getGridProjection();
		assertDoTask(appmaster, null, null, null, null, "foo");

		// start
		appmaster.startContainerCluster("foo");
		assertDoTask(appmaster, 1, 0, 0, 0, "foo");

		// allocate container 1
		Container container1 = allocateContainer(appmaster, 1);
		assertThat(projection.getMembers().size(), is(1));
		assertDoTask(appmaster, null, null, null, null, "foo");

		// modify - ramp up to 2
		appmaster.modifyContainerCluster("foo", new ProjectionData(2));
		assertDoTask(appmaster, 1, 0, 0, 0, "foo");

		// allocate container 2
		allocateContainer(appmaster, 2);
		assertThat(projection.getMembers().size(), is(2));
		assertDoTask(appmaster, null, null, null, null, "foo");

		// modify - ramp up to 4
		appmaster.modifyContainerCluster("foo", new ProjectionData(4));
		assertDoTask(appmaster, 2, 0, 0, 0, "foo");

		// allocate container 3 and 4
		allocateContainer(appmaster, 3);
		allocateContainer(appmaster, 4);
		assertThat(projection.getMembers().size(), is(4));
		assertDoTask(appmaster, null, null, null, null, "foo");

		releaseContainer(appmaster, container1);
		assertThat(projection.getMembers().size(), is(3));
		assertDoTask(appmaster, 1, 0, 0, 0, "foo");
	}

	@Test
	public void testReleaseShouldReAllocate() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);
		ProjectionData projectionData = new ProjectionData(1, null, null, "default", 0);
		projectionData.setMemory(0L);
		projectionData.setVirtualCores(0);

		// create and start
		GridProjection projection = appmaster.createContainerCluster("foo", projectionData).getGridProjection();
		appmaster.startContainerCluster("foo");
		assertDoTask(appmaster, 1, 0, 0, 0, "foo");

		// allocate container 1
		Container container1 = allocateContainer(appmaster, 1);
		assertThat(projection.getMembers().size(), is(1));
		appmaster.resetTestData();
		assertDoTask(appmaster, null, null, null, null, "foo");

		// release, should re-allocate
		releaseContainer(appmaster, container1);
		assertThat(projection.getMembers().size(), is(0));
		assertDoTask(appmaster, 1, 0, 0, 0, "foo");
	}

	@Test
	public void testCreateStartStopDestroyOneCluster() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);
		ProjectionData projectionData = new ProjectionData(1, null, null, "default", 0);
		appmaster.createContainerCluster("foo", projectionData);
		appmaster.startContainerCluster("foo");
		appmaster.stopContainerCluster("foo");
		assertDoTask(appmaster, 0, 0, 0, 0, "foo");
		appmaster.destroyContainerCluster("foo");
		assertThat(appmaster.getContainerClusters().size(), is(0));
	}

	@Test
	public void testCreateStartStopStartOneCluster() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);
		ProjectionData projectionData = new ProjectionData(1, null, null, "default", 0);
		appmaster.createContainerCluster("foo", projectionData);
		appmaster.startContainerCluster("foo");
		assertThat(appmaster.getContainerClusters().get("foo").getStateMachine().getState().getId(), is(ClusterState.RUNNING));
		assertDoTask(appmaster, 1, 0, 0, 0, "foo");
		appmaster.stopContainerCluster("foo");
		assertThat(appmaster.getContainerClusters().get("foo").getStateMachine().getState().getId(), is(ClusterState.STOPPED));
		assertDoTask(appmaster, 0, 0, 0, 0, "foo");
		appmaster.startContainerCluster("foo");
		assertThat(appmaster.getContainerClusters().get("foo").getStateMachine().getState().getId(), is(ClusterState.RUNNING));
		assertDoTask(appmaster, 1, 0, 0, 0, "foo");
		appmaster.destroyContainerCluster("foo");
		assertThat(appmaster.getContainerClusters().size(), is(1));
	}

	@Test
	public void testCreateDestroyOneCluster() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);
		ProjectionData projectionData = new ProjectionData(1, null, null, "default", 0);
		appmaster.createContainerCluster("foo", projectionData);
		assertThat(appmaster.getContainerClusters().size(), is(1));
		appmaster.destroyContainerCluster("foo");
		assertThat(appmaster.getContainerClusters().size(), is(0));
	}

	@Test
	public void testModifyClusterWithDifferentConfig() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);
		Map<String, Integer> hosts = new HashMap<String, Integer>();
		hosts.put("host1", 2);
		hosts.put("host2", 2);
		ProjectionData projectionData = new ProjectionData(0, hosts, null, "default", 0);
		projectionData.setMemory(0L);
		projectionData.setVirtualCores(0);

		// create
		GridProjection projection = appmaster.createContainerCluster("foo", projectionData).getGridProjection();
		assertDoTask(appmaster, null, null, null, null, "foo");

		// start
		appmaster.startContainerCluster("foo");
		assertDoTask(appmaster, 0, 2, 0, 0, "foo");

		// allocate container 1
		allocateContainer(appmaster, 1, "host1");
		assertThat(projection.getMembers().size(), is(1));

		// allocate container 2
		allocateContainer(appmaster, 2, "host1");
		assertThat(projection.getMembers().size(), is(2));

		// allocate container 3
		allocateContainer(appmaster, 3, "host2");
		assertThat(projection.getMembers().size(), is(3));

		// allocate container 4
		allocateContainer(appmaster, 4, "host2");
		assertThat(projection.getMembers().size(), is(4));

		assertDoTask(appmaster, null, null, null, null, "foo");

		// modify - set only host1
		hosts.clear();
		hosts.put("host1", 1);
		appmaster.modifyContainerCluster("foo", new ProjectionData(0,hosts,null));

		SatisfyStateData satisfyState = appmaster.getContainerClusters().get("foo").getGridProjection().getSatisfyState();
		assertThat(satisfyState.getRemoveData().size(), is(3));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreateUndefinedCluster() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);
		ProjectionData projectionData = new ProjectionData(1, null, null, "doesnotexist", null);
		appmaster.createContainerCluster("doesnotexist", projectionData);
	}

	@Test
	public void testReleaseNonAcceptedContainer() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);
		Map<String, Integer> hosts = new HashMap<String, Integer>();
		hosts.put("host1", 1);
		ProjectionData projectionData = new ProjectionData(0, hosts, null, "default", 0);
		projectionData.setMemory(0L);
		projectionData.setVirtualCores(0);

		GridProjection projection = appmaster.createContainerCluster("foo", projectionData).getGridProjection();
		assertDoTask(appmaster, null, null, null, null, "foo");

		// we hook into statemachine to listen its events
		TestStateMachineListener sml = new TestStateMachineListener();
		StateMachine<ClusterState, ClusterEvent> stateMachine = appmaster.getContainerClusters().get("foo").getStateMachine();
		stateMachine.addStateListener(sml);

		appmaster.startContainerCluster("foo");
		assertDoTask(appmaster, 0, 1, 0, 0, "foo");

		Container container1 = allocateContainer(appmaster, 1, "host2");
		assertThat(projection.getMembers().size(), is(0));
		assertThat(sml.states.size(), greaterThan(0));
		sml.states.clear();

		// when non of a clusters accept container,
		// check that we get events indicating that
		// clusters are requested to go into
		// an allocation mode.
		releaseContainer(appmaster, container1);
		assertThat(sml.states.size(), greaterThan(0));
		assertDoTask(appmaster, 0, 1, 0, 0, "foo");
		assertThat(appmaster.getContainerClusters().get("foo").getStateMachine().getState().getId(), is(ClusterState.RUNNING));

		allocateContainer(appmaster, 2, "host1");
		assertThat(projection.getMembers().size(), is(1));
		assertThat(sml.states.size(), greaterThan(0));
	}

	private static class TestStateMachineListener extends StateMachineListenerAdapter<ClusterState,ClusterEvent> {

		ArrayList<State<ClusterState, ClusterEvent>> states = new ArrayList<State<ClusterState,ClusterEvent>>();

		@Override
		public void stateChanged(State<ClusterState, ClusterEvent> from, State<ClusterState, ClusterEvent> to) {
			states.add(to);
		}

	}

}
