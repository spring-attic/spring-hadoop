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

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
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
import org.springframework.yarn.am.cluster.ContainerCluster;
import org.springframework.yarn.am.cluster.ContainerClusterStateMachineConfiguration;
import org.springframework.yarn.am.cluster.ManagedContainerClusterAppmaster;
import org.springframework.yarn.am.container.ContainerLauncher;
import org.springframework.yarn.am.grid.GridProjection;
import org.springframework.yarn.am.grid.GridProjectionFactory;
import org.springframework.yarn.am.grid.GridProjectionFactoryLocator;
import org.springframework.yarn.am.grid.support.DefaultGridProjection;
import org.springframework.yarn.am.grid.support.DefaultGridProjectionFactory;
import org.springframework.yarn.am.grid.support.GridProjectionFactoryRegistry;
import org.springframework.yarn.am.grid.support.ProjectionData;
import org.springframework.yarn.am.grid.support.ProjectionDataRegistry;
import org.springframework.yarn.boot.MockUtils;
import org.springframework.yarn.boot.TestUtils;
import org.springframework.yarn.boot.actuate.endpoint.YarnContainerClusterEndpoint;
import org.springframework.yarn.boot.actuate.endpoint.mvc.YarnContainerClusterWithCustomProjectionsMvcEndpointTests.TestConfiguration;
import org.springframework.yarn.listener.ContainerAllocatorListener;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = { TestConfiguration.class })
@WebAppConfiguration
@DirtiesContext(classMode=ClassMode.AFTER_EACH_TEST_METHOD)
public class YarnContainerClusterWithCustomProjectionsMvcEndpointTests {

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
	 * Test cluster create using any projection.
	 */
	@Test
	public void testClusterCreateCustomProjection() throws Exception {
		String content = "{\"clusterId\":\"cluster1\",\"clusterDef\":\"cluster1\",\"projection\":\"custom\",\"projectionData\":{\"any\":1}},\"extraProperties\":{\"key1\":\"value1\"}}";
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
			registry.addGridProjectionFactory(new TestGridProjectionFactory());
			return registry;
		}

		@Bean
		public ProjectionDataRegistry projectionDataRegistry() {
			Map<String, ProjectionData> defaults = new HashMap<String, ProjectionData>();
			defaults.put("cluster1", new ProjectionData(null, null, null, "any", 0));
			return new ProjectionDataRegistry(defaults);
		}
		
		@Bean
        public ParameterValueMapper conversionServiceParameterValueMapper() {
          return new ConversionServiceParameterValueMapper();
        }

	}

	protected static class TestGridProjectionFactory implements GridProjectionFactory {

		public TestGridProjectionFactory() {
		}

		@Override
		public GridProjection getGridProjection(ProjectionData projectionData, org.apache.hadoop.conf.Configuration configuration) {
			GridProjection projection = null;
			if ("custom".equalsIgnoreCase(projectionData.getType())) {
				DefaultGridProjection p = new DefaultGridProjection();
				p.setPriority(projectionData.getPriority());
				projection = p;
			}
			if (projection != null) {
				projection.setProjectionData(projectionData);
			}
			return projection;
		}

		@Override
		public Set<String> getRegisteredProjectionTypes() {
			return new HashSet<String>(Arrays.asList("custom"));
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

}
