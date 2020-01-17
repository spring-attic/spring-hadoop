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
package org.springframework.yarn.boot.actuate.endpoint.mvc;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointAutoConfiguration;
import org.springframework.boot.actuate.endpoint.invoke.ParameterValueMapper;
import org.springframework.boot.actuate.endpoint.invoke.convert.ConversionServiceParameterValueMapper;
import org.springframework.boot.autoconfigure.hateoas.HypermediaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.allocate.ContainerAllocateData;
import org.springframework.yarn.am.allocate.ContainerAllocator;
import org.springframework.yarn.am.cluster.ClusterState;
import org.springframework.yarn.am.cluster.ContainerCluster;
import org.springframework.yarn.am.cluster.ContainerClusterStateMachineConfiguration;
import org.springframework.yarn.am.cluster.ManagedContainerClusterAppmaster;
import org.springframework.yarn.am.container.ContainerLauncher;
import org.springframework.yarn.am.grid.GridProjectionFactoryLocator;
import org.springframework.yarn.am.grid.support.DefaultGridProjection;
import org.springframework.yarn.am.grid.support.DefaultGridProjectionFactory;
import org.springframework.yarn.am.grid.support.GridProjectionFactoryRegistry;
import org.springframework.yarn.am.grid.support.ProjectionData;
import org.springframework.yarn.am.grid.support.ProjectionDataRegistry;
import org.springframework.yarn.boot.MockUtils;
import org.springframework.yarn.boot.TestUtils;
import org.springframework.yarn.boot.actuate.endpoint.YarnContainerClusterEndpoint;
import org.springframework.yarn.boot.actuate.endpoint.mvc.YarnContainerClusterMvcEndpointTests.TestConfiguration;
import org.springframework.yarn.listener.ContainerAllocatorListener;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = { TestConfiguration.class })
@WebAppConfiguration
@DirtiesContext(classMode=ClassMode.AFTER_EACH_TEST_METHOD)
public class YarnContainerClusterMvcEndpointTests {

	private final static String BASE = "/" + YarnContainerClusterEndpoint.ENDPOINT_ID;

	@Autowired
	private WebApplicationContext context;

	private MockMvc mvc;

	private TestYarnAppmaster appmaster;

	@Before
	public void setUp() {
		mvc = MockMvcBuilders.webAppContextSetup(context).build();
		appmaster = (TestYarnAppmaster) context.getBean(YarnAppmaster.class);
	}

	/**
	 * Test home GET when no clusters has been defined.
	 */
	@Test
	public void testHomeEmpty() throws Exception {
		mvc.
			perform(get(BASE)).
			andExpect(status().isOk()).
			andExpect(content().string(not(emptyString()))).
			andExpect(jsonPath("$.*", hasSize(1))).
			andExpect(jsonPath("$.clusters", hasSize(0)));
	}

	/**
	 * Test cluster create using any projection.
	 */
	@Test
	public void testClusterCreateAnyProjection() throws Exception {
		String content = "{\"clusterId\":\"cluster1\",\"clusterDef\":\"cluster1\",\"projection\":\"DEFAULT\",\"projectionData\":{\"any\":1}},\"extraProperties\":{\"key1\":\"value1\"}}";
		mvc.
			perform(post(BASE).content(content).contentType(MediaType.APPLICATION_JSON)).
			andExpect(status().isCreated()).
			andExpect(content().string(is(""))).
			andExpect(header().string("Location", endsWith("/cluster1")));
		Map<String, ContainerCluster> clusters = TestUtils.readField("clusters", appmaster);
		assertThat(clusters.size(), is(1));
		assertThat(clusters.containsKey("cluster1"), is(true));
		assertThat(clusters.get("cluster1").getGridProjection().getProjectionData().getAny(), is(1));
	}

	/**
	 * Test cluster create using hosts projection.
	 */
	@Test
	public void testClusterCreateHostsProjection() throws Exception {
		String content = "{\"clusterId\":\"cluster1\",\"projection\":\"DEFAULT\",\"projectionData\":{\"any\":1,\"hosts\":{\"host1\":11,\"host2\":22}}}";
		mvc.
			perform(post(BASE).content(content).contentType(MediaType.APPLICATION_JSON)).
			andExpect(status().isCreated()).
			andExpect(content().string(is(""))).
			andExpect(header().string("Location", endsWith("/cluster1")));

		Map<String, ContainerCluster> clusters = TestUtils.readField("clusters", appmaster);
		assertThat(clusters.size(), is(1));
		assertThat(clusters.containsKey("cluster1"), is(true));
		assertThat(clusters.get("cluster1").getGridProjection(), instanceOf(DefaultGridProjection.class));
		assertThat(clusters.get("cluster1").getGridProjection().getSatisfyState().getAllocateData().getAny(), is(1));
		assertThat(clusters.get("cluster1").getGridProjection().getSatisfyState().getAllocateData().getHosts().size(), is(2));
		assertThat(clusters.get("cluster1").getGridProjection().getSatisfyState().getAllocateData().getHosts().get("host1"), is(11));
		assertThat(clusters.get("cluster1").getGridProjection().getSatisfyState().getAllocateData().getHosts().get("host2"), is(22));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testClusterCreateComplexProjectionData() throws Exception {
		String content = "{\"clusterId\":\"cluster1\",\"clusterDef\":\"cluster1\",\"projection\":\"DEFAULT\",\"projectionData\":{\"any\":1,\"hosts\":{\"host1\":11,\"host2\":22},\"racks\":{\"rack1\":1,\"rack2\":2},\"properties\":{\"key1\":123,\"key2\":\"value2\",\"key3\":{\"subkey\":\"subvalue\"}}}},\"extraProperties\":{\"key1\":\"value1\"}}";
		mvc.
			perform(post(BASE).content(content).contentType(MediaType.APPLICATION_JSON)).
			andExpect(status().isCreated()).
			andExpect(content().string(is(""))).
			andExpect(header().string("Location", endsWith("/cluster1")));
		Map<String, ContainerCluster> clusters = TestUtils.readField("clusters", appmaster);
		assertThat(clusters.size(), is(1));
		assertThat(clusters.containsKey("cluster1"), is(true));
		assertThat(clusters.get("cluster1").getGridProjection(), instanceOf(DefaultGridProjection.class));
		DefaultGridProjection projection = (DefaultGridProjection) clusters.get("cluster1").getGridProjection();
		assertThat(projection.getProjectionData().getAny(), is(1));
		assertThat(projection.getProjectionData().getHosts().get("host1"), is(11));
		assertThat(projection.getProjectionData().getHosts().get("host2"), is(22));
		assertThat(projection.getProjectionData().getRacks().get("rack1"), is(1));
		assertThat(projection.getProjectionData().getRacks().get("rack2"), is(2));
		assertThat((Integer)projection.getProjectionData().getProperties().get("key1"), is(123));
		assertThat((String)projection.getProjectionData().getProperties().get("key2"), is("value2"));
		assertThat((String)((Map<String,Object>)projection.getProjectionData().getProperties().get("key3")).get("subkey"), is("subvalue"));
	}

	/**
	 * Test home GET when one any cluster has been created.
	 */
	@Test
	public void testHomeWithAnyCluster() throws Exception {
		testClusterCreateAnyProjection();

		mvc.
			perform(get(BASE)).
			andExpect(status().isOk()).
			andExpect(content().string(not(emptyString()))).
			andExpect(jsonPath("$.*", hasSize(1))).
			andExpect(jsonPath("$.clusters", hasSize(1))).
			andExpect(jsonPath("$.clusters[0]", is("cluster1")));
	}

	/**
	 * Test home GET when one hosts cluster has been created.
	 */
	@Test
	public void testHomeWithHostsCluster() throws Exception {
		testClusterCreateHostsProjection();

		mvc.
			perform(get(BASE)).
			andExpect(status().isOk()).
			andExpect(content().string(not(emptyString()))).
			andExpect(jsonPath("$.*", hasSize(1))).
			andExpect(jsonPath("$.clusters", hasSize(1))).
			andExpect(jsonPath("$.clusters[0]", is("cluster1")));
	}

	@Test
	public void testClusterWithMember() throws Exception {
		testHomeWithAnyCluster();
		TestUtils.callMethod("doTask", appmaster);

		String content = "{\"action\":\"start\"}";
		mvc.
			perform(put(BASE + "/cluster1").content(content).contentType(MediaType.APPLICATION_JSON)).
			andExpect(status().isOk()).
			andExpect(content().string(emptyString()));

		allocateContainer(appmaster, 1);

		mvc.
			perform(get(BASE + "/cluster1")).
			andExpect(status().isOk()).
			andExpect(content().string(not(emptyString()))).
			andExpect(jsonPath("$.*", hasSize(3))).
			andExpect(jsonPath("$.id", is("cluster1"))).
			andExpect(jsonPath("$.gridProjection.*", hasSize(3))).
			andExpect(jsonPath("$.gridProjection.members", hasSize(1))).
			andExpect(jsonPath("$.gridProjection.projectionData.*", hasSize(9))).
			andExpect(jsonPath("$.gridProjection.projectionData.type", is("default"))).
			andExpect(jsonPath("$.gridProjection.projectionData.priority", is(0))).
			andExpect(jsonPath("$.gridProjection.projectionData.memory", is(0))).
			andExpect(jsonPath("$.gridProjection.projectionData.virtualCores", is(0))).
			andExpect(jsonPath("$.gridProjection.projectionData.locality", nullValue())).
			andExpect(jsonPath("$.gridProjection.projectionData.any", is(1))).
			andExpect(jsonPath("$.gridProjection.projectionData.hosts.*", hasSize(0))).
			andExpect(jsonPath("$.gridProjection.projectionData.racks.*", hasSize(0))).
			andExpect(jsonPath("$.gridProjection.projectionData.properties.*", hasSize(0))).
			andExpect(jsonPath("$.gridProjection.satisfyState.*", hasSize(2))).
			andExpect(jsonPath("$.gridProjection.satisfyState.allocateData.*", hasSize(3))).
			andExpect(jsonPath("$.gridProjection.satisfyState.allocateData.racks.*", hasSize(0))).
			andExpect(jsonPath("$.gridProjection.satisfyState.allocateData.any", is(0))).
			andExpect(jsonPath("$.gridProjection.satisfyState.allocateData.hosts.*", hasSize(0))).
			andExpect(jsonPath("$.gridProjection.satisfyState.removeData", hasSize(0))).
			andExpect(jsonPath("$.containerClusterState.clusterState", is(ClusterState.RUNNING.toString())));
	}

	@Test
	public void testStartCluster() throws Exception {
		String content = "{\"clusterId\":\"foo\",\"projection\":\"DEFAULT\",\"projectionData\":{\"any\":1}}";
		mvc.
			perform(post(BASE).content(content).contentType(MediaType.APPLICATION_JSON));

		content = "{\"action\":\"start\"}";
		mvc.
			perform(put(BASE + "/foo").content(content).contentType(MediaType.APPLICATION_JSON)).
			andExpect(status().isOk()).
			andExpect(content().string(emptyString()));

		Map<String, ContainerCluster> clusters = TestUtils.readField("clusters", appmaster);
		assertThat(clusters.size(), is(1));
		assertThat(clusters.containsKey("foo"), is(true));
		assertThat(clusters.get("foo").getStateMachine().getState().getId(), is(ClusterState.RUNNING));
	}

	@Test
	public void testStartClusterDoesNotExist() throws Exception {
		String content = "{\"action\":\"start\"}";
		mvc.
			perform(put(BASE + "/foo").content(content).contentType(MediaType.APPLICATION_JSON)).
			andExpect(status().isNotFound()).
			andExpect(status().reason("No such cluster"));
	}

	@Test
	public void testStopCluster() throws Exception {
		testStartCluster();
		String content = "{\"action\":\"stop\"}";
		mvc.
		perform(put(BASE + "/foo").content(content).contentType(MediaType.APPLICATION_JSON)).
		andExpect(status().isOk()).
		andExpect(content().string(emptyString()));
		Map<String, ContainerCluster> clusters = TestUtils.readField("clusters", appmaster);
		assertThat(clusters.size(), is(1));
		assertThat(clusters.containsKey("foo"), is(true));
		assertThat(clusters.get("foo").getStateMachine().getState().getId(), is(ClusterState.STOPPED));
	}

	@Test
	public void testClusterStatus() throws Exception {
		testClusterCreateHostsProjection();
		mvc.
			perform(get(BASE + "/cluster1")).
			andExpect(status().isOk()).
			andExpect(content().string(containsString("cluster1")));

	}

	@Test
	public void testClusterModify() throws Exception {
		testStartCluster();
		String content = "{\"clusterId\":\"foo\",\"projectionData\":{\"any\":2}}}";
		mvc.
			perform(patch(BASE + "/foo").content(content).contentType(MediaType.APPLICATION_JSON)).
			andExpect(status().isOk());
		Map<String, ContainerCluster> clusters = TestUtils.readField("clusters", appmaster);
		assertThat(clusters.size(), is(1));
		assertThat(clusters.containsKey("foo"), is(true));
		assertThat(clusters.get("foo").getGridProjection().getProjectionData().getAny(), is(2));
	}

	@Test
	public void testDotsInClusterName() throws Exception {
		String content = "{\"clusterId\":\"cluster.1.1\",\"clusterDef\":\"cluster1\",\"projection\":\"DEFAULT\",\"projectionData\":{\"any\":1}},\"extraProperties\":{\"key1\":\"value1\"}}";
		mvc.
			perform(post(BASE).content(content).contentType(MediaType.APPLICATION_JSON)).
			andExpect(status().isCreated()).
			andExpect(content().string(is(""))).
			andExpect(header().string("Location", endsWith("/cluster.1.1")));
		Map<String, ContainerCluster> clusters = TestUtils.readField("clusters", appmaster);
		assertThat(clusters.size(), is(1));
		assertThat(clusters.containsKey("cluster.1.1"), is(true));
		assertThat(clusters.get("cluster.1.1").getGridProjection().getProjectionData().getAny(), is(1));

		mvc.
			perform(get(BASE)).
			andExpect(status().isOk()).
			andExpect(content().string(not(emptyString()))).
			andExpect(jsonPath("$.*", hasSize(1))).
			andExpect(jsonPath("$.clusters", hasSize(1))).
			andExpect(jsonPath("$.clusters[0]", is("cluster.1.1")));

		content = "{\"action\":\"start\"}";
		mvc.
			perform(put(BASE + "/cluster.1.1").content(content).contentType(MediaType.APPLICATION_JSON)).
			andExpect(status().isOk()).
			andExpect(content().string(emptyString()));

		TestUtils.callMethod("doTask", appmaster);
		allocateContainer(appmaster, 1);
		mvc.
			perform(get(BASE + "/cluster.1.1")).
			andExpect(status().isOk()).
			andExpect(content().string(not(emptyString()))).
			andExpect(jsonPath("$.*", hasSize(3))).
			andExpect(jsonPath("$.id", is("cluster.1.1"))).
			andExpect(jsonPath("$.gridProjection.*", hasSize(3))).
			andExpect(jsonPath("$.gridProjection.members", hasSize(1))).
			andExpect(jsonPath("$.gridProjection.projectionData.*", hasSize(9))).
			andExpect(jsonPath("$.gridProjection.projectionData.type", is("default"))).
			andExpect(jsonPath("$.gridProjection.projectionData.priority", is(0))).
			andExpect(jsonPath("$.gridProjection.projectionData.memory", is(0))).
			andExpect(jsonPath("$.gridProjection.projectionData.virtualCores", is(0))).
			andExpect(jsonPath("$.gridProjection.projectionData.locality", nullValue())).
			andExpect(jsonPath("$.gridProjection.projectionData.any", is(1))).
			andExpect(jsonPath("$.gridProjection.projectionData.hosts.*", hasSize(0))).
			andExpect(jsonPath("$.gridProjection.projectionData.racks.*", hasSize(0))).
			andExpect(jsonPath("$.gridProjection.projectionData.properties.*", hasSize(0))).
			andExpect(jsonPath("$.gridProjection.satisfyState.*", hasSize(2))).
			andExpect(jsonPath("$.gridProjection.satisfyState.allocateData.*", hasSize(3))).
			andExpect(jsonPath("$.gridProjection.satisfyState.allocateData.racks.*", hasSize(0))).
			andExpect(jsonPath("$.gridProjection.satisfyState.allocateData.any", is(0))).
			andExpect(jsonPath("$.gridProjection.satisfyState.allocateData.hosts.*", hasSize(0))).
			andExpect(jsonPath("$.gridProjection.satisfyState.removeData", hasSize(0))).
			andExpect(jsonPath("$.containerClusterState.clusterState", is(ClusterState.RUNNING.toString())));
	}


	@Import({ ContainerClusterStateMachineConfiguration.class, WebEndpointAutoConfiguration.class,
			HypermediaAutoConfiguration.class })
	@EnableWebMvc
	@Configuration
	public static class TestConfiguration {

		@Bean
		public YarnContainerClusterEndpoint endpoint() {
			return new YarnContainerClusterEndpoint();
		}

		@Bean
		public YarnContainerClusterMvcEndpoint mvcEndpoint() {
			return new YarnContainerClusterMvcEndpoint(endpoint());
		}

		@Bean
		public YarnAppmaster appMaster() {
			TestContainerAllocator allocator = new TestContainerAllocator();
			TestContainerLauncher launcher = new TestContainerLauncher();
			TestYarnAppmaster appmaster = new TestYarnAppmaster();
			appmaster.setAllocator(allocator);
			appmaster.setLauncher(launcher);
			return appmaster;
		}

		@Bean
		public TaskExecutor taskExecutor() {
			return new SyncTaskExecutor();
		}

		@Bean
		public GridProjectionFactoryLocator gridProjectionFactoryLocator() {
			GridProjectionFactoryRegistry registry = new GridProjectionFactoryRegistry();
			registry.addGridProjectionFactory(new DefaultGridProjectionFactory());
			return registry;
		}

		@Bean
		public ProjectionDataRegistry projectionDataRegistry() {
			Map<String, ProjectionData> defaults = new HashMap<String, ProjectionData>();
			ProjectionData projectionData = new ProjectionData(null, null, null, "any", 0);
			projectionData.setVirtualCores(0);
			projectionData.setMemory(0L);
			defaults.put("cluster1", projectionData);
			return new ProjectionDataRegistry(defaults);
		}
		
		@Bean
        public ParameterValueMapper conversionServiceParameterValueMapper() {
          return new ConversionServiceParameterValueMapper();
        }

	}

	protected static class TestYarnAppmaster extends ManagedContainerClusterAppmaster {
		@Override
		protected void onInit() throws Exception {
			setConfiguration(new org.apache.hadoop.conf.Configuration());
			super.onInit();
		}
		@Override
		protected void doStart() {
		}
		@Override
		protected void doStop() {
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

	protected Container allocateContainer(Object appmaster, int id) throws Exception {
		// force to set host as "host"
		return allocateContainer(appmaster, id, "host");
	}

	protected Container allocateContainer(Object appmaster, int id, String host) throws Exception {
		ContainerId containerId = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 0);
		NodeId nodeId = MockUtils.getMockNodeId(host, 0);
		Priority priority = MockUtils.getMockPriority(0);
		Resource resource = MockUtils.getMockResource(0, 0);
		Container container = MockUtils.getMockContainer(containerId, nodeId, resource, priority);
		TestUtils.callMethod("onContainerAllocated", appmaster, new Object[]{container}, new Class<?>[]{Container.class});
		return container;
	}

}
