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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.junit.After;
import org.junit.Before;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.yarn.MockUtils;
import org.springframework.yarn.TestUtils;
import org.springframework.yarn.am.allocate.ContainerAllocateData;
import org.springframework.yarn.am.allocate.ContainerAllocator;
import org.springframework.yarn.am.container.ContainerLauncher;
import org.springframework.yarn.am.grid.support.DefaultGridProjectionFactory;
import org.springframework.yarn.am.grid.support.SatisfyStateData;
import org.springframework.yarn.listener.ContainerAllocatorListener;
import org.springframework.yarn.support.statemachine.StateMachineSystemConstants;
import org.springframework.yarn.support.statemachine.config.EnumStateMachineFactory;

public abstract class AbstractManagedContainerClusterAppmasterTests {

	private AnnotationConfigApplicationContext ctx;
	protected EnumStateMachineFactory<ClusterState, ClusterEvent> stateMachineFactory;

	protected Container allocateContainer(Object appmaster, int id) throws Exception {
		return allocateContainer(appmaster, id, null);
	}

	protected Container allocateContainer(Object appmaster, int id, String host) throws Exception {
		ContainerId containerId = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 0);
		NodeId nodeId = MockUtils.getMockNodeId(host, 0);
		Priority priority = MockUtils.getMockPriority(0);
		Container container = MockUtils.getMockContainer(containerId, nodeId, null, priority);
		TestUtils.callMethod("onContainerAllocated", appmaster, new Object[]{container}, new Class<?>[]{Container.class});
		return container;
	}

	protected void releaseContainer(Object appmaster, Container container) throws Exception {
		ContainerStatus containerStatus = MockUtils.getMockContainerStatus(container.getId(), ContainerState.COMPLETE, 0);
		TestUtils.callMethod("onContainerCompleted", appmaster, new Object[]{containerStatus}, new Class<?>[]{ContainerStatus.class});
	}

	protected void assertDoTask(TestManagedContainerClusterAppmaster appmaster, Integer anySize, Integer hostsSize, Integer racksSize, Integer killSize, String cluster) throws Exception {
		assertDoTask(appmaster, anySize, hostsSize, racksSize, killSize, cluster, false);
	}

	protected void assertDoTask(TestManagedContainerClusterAppmaster appmaster, Integer anySize, Integer hostsSize, Integer racksSize, Integer killSize, String cluster, boolean notExist) throws Exception {
		TestUtils.callMethod("doTask", appmaster);
		if (notExist) {
			assertThat(appmaster.getSatisfyStateDataByCluster(cluster), nullValue());
			return;
		}
		if (anySize != null || hostsSize != null || racksSize != null) {
			assertThat(appmaster.getSatisfyStateDataByCluster(cluster).getAllocateData(), notNullValue());

		}
		if (anySize != null) {
			assertThat(appmaster.getSatisfyStateDataByCluster(cluster).getAllocateData().getAny(), is(anySize));
		}
		if (hostsSize != null) {
			assertThat(appmaster.getSatisfyStateDataByCluster(cluster).getAllocateData().getHosts().size(), is(hostsSize));
		}
		if (racksSize != null) {
			assertThat(appmaster.getSatisfyStateDataByCluster(cluster).getAllocateData().getRacks().size(), is(racksSize));
		}
		if (killSize != null) {
			assertThat(appmaster.getSatisfyStateDataByCluster(cluster).getRemoveData(), notNullValue());
			assertThat(appmaster.getSatisfyStateDataByCluster(cluster).getRemoveData().size(), is(killSize));
		} else {
//			assertThat(appmaster.getSatisfyStateDataByCluster(cluster), nullValue());
//			assertThat(appmaster.getSatisfyStateDataByCluster(cluster), notNullValue());
		}
	}

//	protected static void assertSatisfyStateData(Object appmaster, Integer mapSize, Integer anySize, Integer hostsSize, Integer racksSize) throws Exception {
//		Map<ContainerCluster, SatisfyStateData> map = TestUtils.callMethod("getSatisfyStateData", appmaster);
//		assertThat(map.size(), is(mapSize));
//		SatisfyStateData data = null;
//		if (mapSize != null && mapSize > 0) {
//			data = map.entrySet().iterator().next().getValue();
//		}
//		if (anySize != null) {
//			assertThat(data.getAllocateData().getAny(), is(anySize));
//		}
//		if (hostsSize != null) {
//			assertThat(data.getAllocateData().getHosts().size(), is(hostsSize));
//		}
//		if (racksSize != null) {
//			assertThat(data.getAllocateData().getRacks().size(), is(racksSize));
//		}
//	}

	protected static ManagedContainerClusterAppmaster createManagedAppmaster() throws Exception {
		ManagedContainerClusterAppmaster appmaster = new ManagedContainerClusterAppmaster();
		appmaster.setConfiguration(new Configuration());
		appmaster.setGridProjectionFactory(new DefaultGridProjectionFactory(null));
		TestUtils.callMethod("onInit", appmaster);
		return appmaster;
	}

	@SuppressWarnings("unchecked")
	@Before
	public void setup() {
		ctx = new AnnotationConfigApplicationContext(ContainerClusterStateMachineConfiguration.class, TestConfig.class);
		stateMachineFactory = ctx.getBean(StateMachineSystemConstants.DEFAULT_ID_STATEMACHINEFACTORY, EnumStateMachineFactory.class);
	}

	@After
	public void clean() {
		if (ctx != null) {
			try {
				ctx.close();
			} catch (Exception e) {
			}
			ctx = null;
		}
	}

	@org.springframework.context.annotation.Configuration
	public static class TestConfig {
		@Bean
		public TaskExecutor taskExecutor() {
			return new SyncTaskExecutor();
		}
	}

	protected static TestManagedContainerClusterAppmaster createTestAppmaster(TestContainerAllocator allocator, TestContainerLauncher launcher) throws Exception {
		TestManagedContainerClusterAppmaster appmaster = new TestManagedContainerClusterAppmaster();
		appmaster.setTaskExecutor(new SyncTaskExecutor());
		appmaster.setConfiguration(new Configuration());
		appmaster.setAllocator(allocator);
		appmaster.setLauncher(launcher);
		appmaster.setGridProjectionFactory(new DefaultGridProjectionFactory(null));
		TestUtils.callMethod("onInit", appmaster);
		return appmaster;
	}

	protected static class TestManagedContainerClusterAppmaster extends ManagedContainerClusterAppmaster {

		Map<ContainerCluster, SatisfyStateData> satisfyStateData = new HashMap<ContainerCluster, SatisfyStateData>();

		public SatisfyStateData getSatisfyStateDataByCluster(String cluster) {
			if (satisfyStateData == null) {
				return null;
			}
			for (Entry<ContainerCluster, SatisfyStateData> entry : satisfyStateData.entrySet()) {
				if (entry.getKey().getId().equals(cluster)) {
					return entry.getValue();
				}
			}
			return null;
		}

		public void resetTestData() {
//			satisfyStateData = null;
			satisfyStateData.clear();;
		}

//		@Override
//		protected void handleSatisfyStateData(Map<ContainerCluster, SatisfyStateData> satisfyData) {
//			satisfyStateData = satisfyData;
//		}

		@Override
		protected void handleSatisfyStateData(ContainerCluster cluster, SatisfyStateData satisfyData) {
			satisfyStateData.put(cluster, satisfyData);
		}

	}

	protected static class TestContainerAllocator implements ContainerAllocator {

		ArrayList<ContainerAllocateData> containerAllocateData;
		ArrayList<Container> releaseContainers;

		public void resetTestData() {
			containerAllocateData = null;
			releaseContainers = null;
		}

		@Override
		public void allocateContainers(int count) {
		}

		@Override
		public void allocateContainers(ContainerAllocateData containerAllocateData) {
			if (this.containerAllocateData == null) {
				this.containerAllocateData = new ArrayList<ContainerAllocateData>();
			}
			this.containerAllocateData.add(containerAllocateData);
		}

		@Override
		public void releaseContainers(List<Container> containers) {
			if (this.releaseContainers == null) {
				this.releaseContainers = new ArrayList<Container>();
			}
			this.releaseContainers.addAll(containers);
		}

		@Override
		public void releaseContainer(ContainerId containerId) {
		}

		@Override
		public void addListener(ContainerAllocatorListener listener) {
		}

		@Override
		public void setProgress(float progress) {
		}

	}

	protected static class TestContainerLauncher implements ContainerLauncher {

		Container container;

		public void resetTestData() {
			container = null;
		}

		@Override
		public void launchContainer(Container container, List<String> commands) {
			this.container = container;
		}

	}

}
