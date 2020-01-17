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
package org.springframework.yarn.am.cluster;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.yarn.TestUtils;
import org.springframework.yarn.am.grid.GridProjection;
import org.springframework.yarn.am.grid.support.ProjectionData;

/**
 * Tests for {@link ManagedContainerClusterAppmaster} using multiple clusters.
 *
 * @author Janne Valkealahti
 *
 */
public class ManagedContainerClusterAppmasterMultiTests extends AbstractManagedContainerClusterAppmasterTests {

	@Test
	public void testCreateTwoClusters() throws Exception {
		ManagedContainerClusterAppmaster appmaster = createManagedAppmaster();
		appmaster.setStateMachineFactory(stateMachineFactory);

		// cluster1
		ProjectionData projectionData1 = new ProjectionData(1, null, null, "default", 0);
		appmaster.createContainerCluster("cluster1", projectionData1);

		// cluster2
		ProjectionData projectionData2 = new ProjectionData(1, null, null, "default", 0);
		appmaster.createContainerCluster("cluster2", projectionData2);
	}

	@Test
	public void testCreateStartTwoClustersDoAllocation() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);

		ProjectionData projectionData1 = new ProjectionData(1, null, null, "default", 0);
		projectionData1.setMemory(0L);
		projectionData1.setVirtualCores(0);
		appmaster.createContainerCluster("cluster1", projectionData1);
		appmaster.startContainerCluster("cluster1");

		ProjectionData projectionData2 = new ProjectionData(1, null, null, "default", 0);
		projectionData2.setMemory(0L);
		projectionData2.setVirtualCores(0);
		appmaster.createContainerCluster("cluster2", projectionData2);
		appmaster.startContainerCluster("cluster2");


		// should get 1 any alloc and no container kills
		TestUtils.callMethod("doTask", appmaster);
		assertThat(appmaster.satisfyStateData.size(), is(2));

		assertThat(appmaster.getSatisfyStateDataByCluster("cluster1").getAllocateData().getAny(), is(1));
		assertThat(appmaster.getSatisfyStateDataByCluster("cluster2").getAllocateData().getAny(), is(1));
		assertThat(appmaster.getSatisfyStateDataByCluster("cluster1").getRemoveData().size(), is(0));
		assertThat(appmaster.getSatisfyStateDataByCluster("cluster2").getRemoveData().size(), is(0));

		// allocate 1
		appmaster.resetTestData();
		allocateContainer(appmaster, 1);
		TestUtils.callMethod("doTask", appmaster);
		assertThat(launcher.container, notNullValue());
		assertThat(allocator.containerAllocateData, nullValue());
		assertThat(allocator.releaseContainers, nullValue());
		launcher.resetTestData();
		allocator.resetTestData();

		// allocate 2
		appmaster.resetTestData();
		allocateContainer(appmaster, 1);
		TestUtils.callMethod("doTask", appmaster);
		assertThat(launcher.container, notNullValue());
		assertThat(allocator.containerAllocateData, nullValue());
		assertThat(allocator.releaseContainers, nullValue());
		launcher.resetTestData();
		allocator.resetTestData();
	}

	@Test
	public void testAllocateMatchCorrectClusterAfterOnlyOneStarted() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);

		Map<String, Object> extraProperties1 = new HashMap<String, Object>();
		extraProperties1.put("key", "value1");
		ProjectionData projectionData1 = new ProjectionData(1, null, null, "default", 0);
		projectionData1.setMemory(0L);
		projectionData1.setVirtualCores(0);
		appmaster.createContainerCluster("cluster1", "cluster", projectionData1, extraProperties1);

		Map<String, Object> extraProperties2 = new HashMap<String, Object>();
		extraProperties2.put("key", "value2");
		ProjectionData projectionData2 = new ProjectionData(1, null, null, "default", 0);
		projectionData2.setMemory(0L);
		projectionData2.setVirtualCores(0);
		appmaster.createContainerCluster("cluster2", "cluster", projectionData2, extraProperties2);

		appmaster.startContainerCluster("cluster2");

		// should get 1 any alloc and no container kills
		TestUtils.callMethod("doTask", appmaster);
		assertThat(appmaster.satisfyStateData.size(), is(1));

		// allocate 1
		appmaster.resetTestData();
		allocateContainer(appmaster, 1);
		TestUtils.callMethod("doTask", appmaster);
		assertThat(launcher.container, notNullValue());
		assertThat(allocator.containerAllocateData, nullValue());
		assertThat(allocator.releaseContainers, nullValue());
		assertThat(appmaster.onContainerLaunchCommandsData.size(), is(1));
		assertThat(appmaster.onContainerLaunchCommandsData.get(0).getId(), is("cluster2"));
		launcher.resetTestData();
		allocator.resetTestData();

		appmaster.startContainerCluster("cluster1");

		// allocate 2
		appmaster.resetTestData();
		allocateContainer(appmaster, 1);
		TestUtils.callMethod("doTask", appmaster);
		assertThat(launcher.container, notNullValue());
		assertThat(allocator.containerAllocateData, nullValue());
		assertThat(allocator.releaseContainers, nullValue());
		assertThat(appmaster.onContainerLaunchCommandsData.size(), is(1));
		assertThat(appmaster.onContainerLaunchCommandsData.get(0).getId(), is("cluster1"));
		launcher.resetTestData();
		allocator.resetTestData();
	}

	@Test
	public void testCreateStartModifyTwoAnyClusters() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);

		// cluster1
		ProjectionData projectionData1 = new ProjectionData(1, null, null, "default", 0);
		projectionData1.setMemory(0L);
		projectionData1.setVirtualCores(0);
		GridProjection projection1 = appmaster.createContainerCluster("cluster1", projectionData1).getGridProjection();

		// cluster2
		ProjectionData projectionData2 = new ProjectionData(1, null, null, "default", 0);
		projectionData2.setMemory(0L);
		projectionData2.setVirtualCores(0);
		GridProjection projection2 = appmaster.createContainerCluster("cluster2", projectionData2).getGridProjection();

		appmaster.startContainerCluster("cluster1");
		assertDoTask(appmaster, 1, 0, 0, 0, "cluster1");
		assertDoTask(appmaster, null, null, null, null, "cluster2");

		appmaster.startContainerCluster("cluster2");
		assertDoTask(appmaster, 1, 0, 0, 0, "cluster2");
		assertDoTask(appmaster, null, null, null, null, "cluster1");

		// allocate container 1
		allocateContainer(appmaster, 1);
		assertThat(projection1.getMembers().size() + projection2.getMembers().size(), is(1));

		allocateContainer(appmaster, 2);
		assertThat(projection1.getMembers().size() + projection2.getMembers().size(), is(2));

		allocateContainer(appmaster, 3);
		assertThat(projection1.getMembers().size(), is(1));
		assertThat(projection2.getMembers().size(), is(1));

		// modify - ramp up to 2
		appmaster.modifyContainerCluster("cluster1", new ProjectionData(2));
		assertDoTask(appmaster, 1, 0, 0, 0, "cluster1");


	}

	@Test
	public void testCreateStartModifyTwoHostsClusters() throws Exception {
		TestContainerAllocator allocator = new TestContainerAllocator();
		TestContainerLauncher launcher = new TestContainerLauncher();
		TestManagedContainerClusterAppmaster appmaster = createTestAppmaster(allocator, launcher);
		appmaster.setStateMachineFactory(stateMachineFactory);

		// cluster1
		Map<String, Integer> hosts1 = new HashMap<String, Integer>();
		hosts1.put("host10", 1);

		ProjectionData projectionData1 = new ProjectionData(0, hosts1, null, "default", 0);
		projectionData1.setMemory(0L);
		projectionData1.setVirtualCores(0);

		GridProjection projection1 = appmaster.createContainerCluster("cluster1", projectionData1).getGridProjection();

		// cluster2
		Map<String, Integer> hosts2 = new HashMap<String, Integer>();
		hosts2.put("host20", 1);
		ProjectionData projectionData2 = new ProjectionData(0, hosts2, null, "default", 0);
		projectionData2.setMemory(0L);
		projectionData2.setVirtualCores(0);
		GridProjection projection2 = appmaster.createContainerCluster("cluster2", projectionData2).getGridProjection();

		appmaster.startContainerCluster("cluster1");
		assertDoTask(appmaster, 0, 1, 0, 0, "cluster1");
		assertDoTask(appmaster, null, null, null, null, "cluster2");

		appmaster.startContainerCluster("cluster2");
		assertDoTask(appmaster, 0, 1, 0, 0, "cluster2");
		assertDoTask(appmaster, null, null, null, null, "cluster1");

		// allocate container 1
		allocateContainer(appmaster, 1, "host10");
		assertThat(projection1.getMembers().size() + projection2.getMembers().size(), is(1));

		allocateContainer(appmaster, 2, "host20");
		assertThat(projection1.getMembers().size() + projection2.getMembers().size(), is(2));

		allocateContainer(appmaster, 3, "hostX1");
		assertThat(projection1.getMembers().size(), is(1));
		assertThat(projection2.getMembers().size(), is(1));
	}


}
