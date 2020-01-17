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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.RackResolver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.yarn.MockUtils;
import org.springframework.yarn.TestUtils;
import org.springframework.yarn.am.grid.GridProjection;

/**
 * Tests for {@link DefaultGridProjection}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultGridProjectionTests {

	@Before
	public void setup() throws Exception {
		// try to fix some hadoop static init usage
		// do it again after a test
		TestUtils.setField("initCalled", RackResolver.class, false);
	}

	@After
	public void clean() throws Exception {
		TestUtils.setField("initCalled", RackResolver.class, false);
	}

	@Test
	public void testAnyDefaults() {
		GridProjection projection = new DefaultGridProjection();
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
	public void testAnyAddOne() {
		GridProjection projection = new DefaultGridProjection();
		projection.setProjectionData(new ProjectionData(1));
		SatisfyStateData satisfyState = projection.getSatisfyState();
		assertThat(satisfyState, notNullValue());
		assertThat(satisfyState.getAllocateData(), notNullValue());
		assertThat(satisfyState.getAllocateData().getAny(), is(1));
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(0));
		assertThat(satisfyState.getAllocateData().getRacks().size(), is(0));
		assertThat(satisfyState.getRemoveData(), notNullValue());
		assertThat(satisfyState.getRemoveData().size(), is(0));
	}

	@Test
	public void testAnyAddOneAddMember() {
		DefaultGridProjection projection = new DefaultGridProjection();
		projection.setProjectionData(new ProjectionData(1));
		projection.setPriority(0);
		projection.setMemory(0L);
		projection.setVirtualCores(0);

		ContainerId id = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 0);
		Priority priority = MockUtils.getMockPriority(0);
		NodeId nodeId = MockUtils.getMockNodeId("host", 0);
		Resource resource1 = MockUtils.getMockResource(0, 0);
		Container container = MockUtils.getMockContainer(id, nodeId, resource1, priority);
		projection.acceptMember(new DefaultGridMember(container));

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
	public void testAnyAddTwoAddMembersRemoveMember() throws Exception {
		DefaultGridProjection projection = new DefaultGridProjection();
		projection.setPriority(0);
		projection.setProjectionData(new ProjectionData(2));
		projection.setMemory(0L);
		projection.setVirtualCores(0);

		Priority priority = MockUtils.getMockPriority(0);

		ContainerId id1 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 1);
		NodeId nodeId1 = MockUtils.getMockNodeId("host1", 0);
		Resource resource1 = MockUtils.getMockResource(0, 0);
		Container container1 = MockUtils.getMockContainer(id1, nodeId1, resource1, priority);
		DefaultGridMember member1 = new DefaultGridMember(container1);
		projection.acceptMember(member1);

		ContainerId id2 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 2);
		NodeId nodeId2 = MockUtils.getMockNodeId("host2", 0);
		Resource resource2 = MockUtils.getMockResource(0, 0);
		Container container2 = MockUtils.getMockContainer(id2, nodeId2, resource2, priority);
		DefaultGridMember member2 = new DefaultGridMember(container2);
		projection.acceptMember(member2);

		SatisfyStateData satisfyState = projection.getSatisfyState();
		assertThat(satisfyState, notNullValue());
		assertThat(satisfyState.getAllocateData(), notNullValue());
		assertThat(satisfyState.getAllocateData().getAny(), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(0));

		projection.setProjectionData(new ProjectionData(1));
		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState, notNullValue());
		assertThat(satisfyState.getAllocateData(), notNullValue());
		assertThat(satisfyState.getAllocateData().getAny(), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(1));

		projection.removeMember(member2);
		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState.getAllocateData().getAny(), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(0));
	}

	@Test
	public void testHostsDefaults() {
		GridProjection projection = new DefaultGridProjection();
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
	public void testHostsAddHosts() throws Exception {
		DefaultGridProjection projection = new DefaultGridProjection();

		ProjectionData projectionData = new ProjectionData();
		projectionData.setHost("host1", 2);
		projectionData.setHost("host2", 2);
		projectionData.setHost("host3", 2);
		projection.setProjectionData(projectionData);
		projection.setPriority(0);
		projection.setMemory(0L);
		projection.setVirtualCores(0);

		SatisfyStateData satisfyState = projection.getSatisfyState();
		assertThat(satisfyState, notNullValue());
		assertThat(satisfyState.getAllocateData(), notNullValue());
		assertThat(satisfyState.getAllocateData().getAny(), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(3));
		assertThat(satisfyState.getAllocateData().getRacks().size(), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(0));

		ContainerId id1 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 1);
		NodeId nodeId1 = MockUtils.getMockNodeId("host1", 0);
		Priority priority = MockUtils.getMockPriority(0);
		Resource resource1 = MockUtils.getMockResource(0, 0);
		Container container1 = MockUtils.getMockContainer(id1, nodeId1, resource1, priority);
		DefaultGridMember member1 = new DefaultGridMember(container1);
		boolean accepted = projection.acceptMember(member1);
		assertThat(accepted, is(true));

		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(3));
		assertThat(satisfyState.getAllocateData().getHosts().get("host1"), is(1));
		assertThat(satisfyState.getAllocateData().getHosts().get("host2"), is(2));
		assertThat(satisfyState.getAllocateData().getHosts().get("host3"), is(2));

		projectionData = new ProjectionData();
		projectionData.setHost("host1", 0);
		projectionData.setHost("host2", 0);
		projectionData.setHost("host3", 0);
		projection.setProjectionData(projectionData);

		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(3));
		assertThat(satisfyState.getAllocateData().getHosts().get("host1"), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().get("host2"), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().get("host3"), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(1));

		projection.removeMember(member1);
		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState.getRemoveData().size(), is(0));
	}

	@Test
	public void testHostsRampDown() throws Exception {
		DefaultGridProjection projection = new DefaultGridProjection();

		ProjectionData projectionData = new ProjectionData();
		projectionData.setHost("host1", 2);
		projectionData.setHost("host2", 2);
		projection.setProjectionData(projectionData);
		projection.setPriority(0);
		projection.setMemory(0L);
		projection.setVirtualCores(0);

		SatisfyStateData satisfyState = projection.getSatisfyState();
		assertThat(satisfyState, notNullValue());
		assertThat(satisfyState.getAllocateData(), notNullValue());
		assertThat(satisfyState.getAllocateData().getAny(), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(2));
		assertThat(satisfyState.getAllocateData().getRacks().size(), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(0));

		ContainerId id1 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 1);
		NodeId nodeId1 = MockUtils.getMockNodeId("host1", 0);
		Priority priority1 = MockUtils.getMockPriority(0);
		Resource resource1 = MockUtils.getMockResource(0, 0);
		Container container1 = MockUtils.getMockContainer(id1, nodeId1, resource1, priority1);
		DefaultGridMember member1 = new DefaultGridMember(container1);
		assertThat(projection.acceptMember(member1), is(true));

		ContainerId id2 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 2);
		NodeId nodeId2 = MockUtils.getMockNodeId("host1", 0);
		Priority priority2 = MockUtils.getMockPriority(0);
		Resource resource2 = MockUtils.getMockResource(0, 0);
		Container container2 = MockUtils.getMockContainer(id2, nodeId2, resource2, priority2);
		DefaultGridMember member2 = new DefaultGridMember(container2);
		assertThat(projection.acceptMember(member2), is(true));

		ContainerId id3 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 3);
		NodeId nodeId3 = MockUtils.getMockNodeId("host2", 0);
		Priority priority3 = MockUtils.getMockPriority(0);
		Resource resource3 = MockUtils.getMockResource(0, 0);
		Container container3 = MockUtils.getMockContainer(id3, nodeId3, resource3, priority3);
		DefaultGridMember member3 = new DefaultGridMember(container3);
		assertThat(projection.acceptMember(member3), is(true));

		ContainerId id4 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 4);
		NodeId nodeId4 = MockUtils.getMockNodeId("host2", 0);
		Priority priority4 = MockUtils.getMockPriority(0);
		Resource resource4 = MockUtils.getMockResource(0, 0);
		Container container4 = MockUtils.getMockContainer(id4, nodeId4, resource4, priority4);
		DefaultGridMember member4 = new DefaultGridMember(container4);
		assertThat(projection.acceptMember(member4), is(true));

		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(2));
		assertThat(satisfyState.getAllocateData().getHosts().get("host1"), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().get("host2"), is(0));

		projectionData = new ProjectionData();
		projectionData.setHost("host1", 1);
		projectionData.setHost("host2", 2);
		projection.setProjectionData(projectionData);

		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(2));
		assertThat(satisfyState.getAllocateData().getHosts().get("host1"), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().get("host2"), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(1));

		projection.removeMember(member2);
		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState.getAllocateData().getHosts().get("host1"), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().get("host2"), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(0));
	}

	@Test
	public void testRacksDefaults() {
		GridProjection projection = new DefaultGridProjection();
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
	public void testRacksAddRacks() throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("net.topology.node.switch.mapping.impl", "org.springframework.yarn.am.grid.support.TestDNSToSwitchMapping");
		DefaultGridProjection projection = new DefaultGridProjection(configuration);
		projection.setPriority(0);
		projection.setMemory(0L);
		projection.setVirtualCores(0);

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
		Resource resource1 = MockUtils.getMockResource(0, 0);
		Container container1 = MockUtils.getMockContainer(id1, nodeId1, resource1, priority);
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

	@Test
	public void testTwoInRackAndOneHostInSameRack() throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("net.topology.node.switch.mapping.impl", "org.springframework.yarn.am.grid.support.TestDNSToSwitchMapping");
		DefaultGridProjection projection = new DefaultGridProjection(configuration);
		projection.setPriority(0);
		projection.setMemory(0L);
		projection.setVirtualCores(0);

		ProjectionData projectionData = new ProjectionData();
		projectionData.setRack("/rack1", 2);
		projectionData.setHost("host1", 1);
		projection.setProjectionData(projectionData);

		SatisfyStateData satisfyState = projection.getSatisfyState();
		assertThat(satisfyState, notNullValue());
		assertThat(satisfyState.getAllocateData(), notNullValue());
		assertThat(satisfyState.getAllocateData().getAny(), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(1));
		assertThat(satisfyState.getAllocateData().getRacks().size(), is(1));
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack1"), is(2));
		assertThat(satisfyState.getAllocateData().getHosts().get("host1"), is(1));
		assertThat(satisfyState.getRemoveData().size(), is(0));

		ContainerId id1 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 1);
		NodeId nodeId1 = MockUtils.getMockNodeId("host1", 0);
		Priority priority = MockUtils.getMockPriority(0);
		Resource resource1 = MockUtils.getMockResource(0, 0);
		Container container1 = MockUtils.getMockContainer(id1, nodeId1, resource1, priority);
		DefaultGridMember member1 = new DefaultGridMember(container1);
		boolean accepted = projection.acceptMember(member1);
		assertThat(accepted, is(true));

		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState, notNullValue());
		assertThat(satisfyState.getAllocateData(), notNullValue());
		assertThat(satisfyState.getAllocateData().getAny(), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(1));
		assertThat(satisfyState.getAllocateData().getRacks().size(), is(1));
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack1"), is(2));
		assertThat(satisfyState.getAllocateData().getHosts().get("host1"), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(0));

		ContainerId id2 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 2);
		NodeId nodeId2 = MockUtils.getMockNodeId("host1", 0);
		Resource resource2 = MockUtils.getMockResource(0, 0);
		Container container2 = MockUtils.getMockContainer(id2, nodeId2, resource2, priority);
		DefaultGridMember member2 = new DefaultGridMember(container2);
		accepted = projection.acceptMember(member2);
		assertThat(accepted, is(true));

		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState, notNullValue());
		assertThat(satisfyState.getAllocateData(), notNullValue());
		assertThat(satisfyState.getAllocateData().getAny(), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(1));
		assertThat(satisfyState.getAllocateData().getRacks().size(), is(1));
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack1"), is(1));
		assertThat(satisfyState.getAllocateData().getHosts().get("host1"), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(0));

		ContainerId id3 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 3);
		NodeId nodeId3 = MockUtils.getMockNodeId("host1", 0);
		Resource resource3 = MockUtils.getMockResource(0, 0);
		Container container3 = MockUtils.getMockContainer(id3, nodeId3, resource3, priority);
		DefaultGridMember member3 = new DefaultGridMember(container3);
		accepted = projection.acceptMember(member3);
		assertThat(accepted, is(true));

		satisfyState = projection.getSatisfyState();
		assertThat(satisfyState, notNullValue());
		assertThat(satisfyState.getAllocateData(), notNullValue());
		assertThat(satisfyState.getAllocateData().getAny(), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().size(), is(1));
		assertThat(satisfyState.getAllocateData().getRacks().size(), is(1));
		assertThat(satisfyState.getAllocateData().getRacks().get("/rack1"), is(0));
		assertThat(satisfyState.getAllocateData().getHosts().get("host1"), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(0));
	}

	@Test
	public void testPrioritySorting() {
		DefaultGridProjection p1 = new DefaultGridProjection();
		p1.setPriority(1);
		DefaultGridProjection p2 = new DefaultGridProjection();
		p2.setPriority(3);
		DefaultGridProjection p3 = new DefaultGridProjection();
		p3.setPriority(2);
		List<GridProjection> list = new ArrayList<GridProjection>();
		list.add(p1);
		list.add(p2);
		list.add(p3);

		Collections.sort(list, GridProjection.PRIORITY_COMPARATOR);
		assertThat(list.get(0), sameInstance((GridProjection)p1));
		assertThat(list.get(1), sameInstance((GridProjection)p3));
		assertThat(list.get(2), sameInstance((GridProjection)p2));
	}

}
