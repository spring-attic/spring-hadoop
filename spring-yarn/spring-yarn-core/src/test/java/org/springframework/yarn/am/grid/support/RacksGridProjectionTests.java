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
package org.springframework.yarn.am.grid.support;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.util.RackResolver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.yarn.MockUtils;
import org.springframework.yarn.TestUtils;
import org.springframework.yarn.am.grid.GridProjection;

/**
 * Tests for {@link RacksGridProjection}.
 *
 * @author Janne Valkealahti
 *
 */
public class RacksGridProjectionTests {

	@Before
	public void setup() throws Exception {
		// try to fix some hadoop static init usage
		// do it again after a test
		TestUtils.setField("initCalled", new RackResolver(), false);
	}

	@After
	public void clean() throws Exception {
		TestUtils.setField("initCalled", new RackResolver(), false);
	}

	@Test
	public void testDefaults() {
		GridProjection projection = new RacksGridProjection();
		SatisfyStateData satisfyState = projection.getSatisfyState();
		assertThat(satisfyState, notNullValue());
		assertThat(satisfyState.getAllocateData(), notNullValue());
		assertThat(satisfyState.getAllocateData().getAny(), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(0));
		assertThat(satisfyState.getAllocateData().getRacks().size(), is(0));
		assertThat(satisfyState.getRemoveData(), notNullValue());
		assertThat(satisfyState.getRemoveData().size(), is(0));
	}

	@Test
	public void testAddRacks() throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("net.topology.node.switch.mapping.impl", "org.springframework.yarn.am.grid.support.TestDNSToSwitchMapping");
		RacksGridProjection projection = new RacksGridProjection(configuration);
		projection.setPriority(0);

		ProjectionData projectionData = new ProjectionData();
		projectionData.setRack("/rack1", 2);
		projectionData.setRack("/rack2", 2);
		projectionData.setRack("/rack3", 2);
		projection.setProjectionData(projectionData);

		SatisfyStateData satisfyState = projection.getSatisfyState();
		assertThat(satisfyState, notNullValue());
		assertThat(satisfyState.getAllocateData(), notNullValue());
		assertThat(satisfyState.getAllocateData().getAny(), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(0));
		assertThat(satisfyState.getAllocateData().getRacks().size(), is(3));
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack1"), is(2));
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack2"), is(2));
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack3"), is(2));
		assertThat(satisfyState.getRemoveData().size(), is(0));

		ContainerId id1 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 1);
		NodeId nodeId1 = MockUtils.getMockNodeId("host1", 0);
		Priority priority = MockUtils.getMockPriority(0);
		Container container1 = MockUtils.getMockContainer(id1, nodeId1, null, priority);
		DefaultGridMember member1 = new DefaultGridMember(container1);
		boolean accepted = projection.acceptMember(member1);
		assertThat(accepted, is(true));

		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().get("host1"), nullValue());
		assertThat(satisfyState.getAllocateData().getHosts().get("host2"), nullValue());
		assertThat(satisfyState.getAllocateData().getHosts().get("host3"), nullValue());
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack1"), is(1));
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack2"), is(2));
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack3"), is(2));

		projectionData = new ProjectionData();
		projectionData.setRack("/rack1", 0);
		projectionData.setRack("/rack2", 0);
		projectionData.setRack("/rack3", 0);
		projection.setProjectionData(projectionData);

		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().get("host1"), nullValue());
		assertThat(satisfyState.getAllocateData().getHosts().get("host2"), nullValue());
		assertThat(satisfyState.getAllocateData().getHosts().get("host3"), nullValue());
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack1"), is(0));
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack2"), is(0));
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack3"), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(1));

		projection.removeMember(member1);
		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState.getRemoveData().size(), is(0));
	}

}
