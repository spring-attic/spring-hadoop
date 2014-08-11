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
import static org.junit.Assert.assertThat;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.junit.Test;
import org.springframework.yarn.MockUtils;
import org.springframework.yarn.am.grid.GridProjection;

/**
 * Tests for {@link HostsGridProjection}.
 *
 * @author Janne Valkealahti
 *
 */
public class HostsGridProjectionTests {

	@Test
	public void testDefaults() {
		GridProjection projection = new HostsGridProjection();
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
	public void testAddHosts() throws Exception {
		HostsGridProjection projection = new HostsGridProjection();

		ProjectionData projectionData = new ProjectionData();
		projectionData.setHost("host1", 2);
		projectionData.setHost("host2", 2);
		projectionData.setHost("host3", 2);
		projection.setProjectionData(projectionData);
		projection.setPriority(0);

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
		Container container1 = MockUtils.getMockContainer(id1, nodeId1, null, priority);
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
	public void testRampDown() throws Exception {
		HostsGridProjection projection = new HostsGridProjection();

		ProjectionData projectionData = new ProjectionData();
		projectionData.setHost("host1", 2);
		projectionData.setHost("host2", 2);
		projection.setProjectionData(projectionData);
		projection.setPriority(0);

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
		Container container1 = MockUtils.getMockContainer(id1, nodeId1, null, priority1);
		DefaultGridMember member1 = new DefaultGridMember(container1);
		assertThat(projection.acceptMember(member1), is(true));

		ContainerId id2 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 2);
		NodeId nodeId2 = MockUtils.getMockNodeId("host1", 0);
		Priority priority2 = MockUtils.getMockPriority(0);
		Container container2 = MockUtils.getMockContainer(id2, nodeId2, null, priority2);
		DefaultGridMember member2 = new DefaultGridMember(container2);
		assertThat(projection.acceptMember(member2), is(true));

		ContainerId id3 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 3);
		NodeId nodeId3 = MockUtils.getMockNodeId("host2", 0);
		Priority priority3 = MockUtils.getMockPriority(0);
		Container container3 = MockUtils.getMockContainer(id3, nodeId3, null, priority3);
		DefaultGridMember member3 = new DefaultGridMember(container3);
		assertThat(projection.acceptMember(member3), is(true));

		ContainerId id4 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 4);
		NodeId nodeId4 = MockUtils.getMockNodeId("host2", 0);
		Priority priority4 = MockUtils.getMockPriority(0);
		Container container4 = MockUtils.getMockContainer(id4, nodeId4, null, priority4);
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

}
