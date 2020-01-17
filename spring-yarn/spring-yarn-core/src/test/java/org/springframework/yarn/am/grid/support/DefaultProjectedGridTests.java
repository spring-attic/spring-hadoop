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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Test;
import org.springframework.yarn.MockUtils;

/**
 * Tests for {@link DefaultProjectedGrid}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultProjectedGridTests {

	@Test
	public void testSimpleOperations() {
		DefaultGrid grid = new DefaultGrid();
		DefaultProjectedGrid projectedGrid = new DefaultProjectedGrid(grid);
		DefaultGridProjection projection = new DefaultGridProjection();
		projection.setProjectionData(new ProjectionData(2));
		projection.setPriority(0);
		projection.setMemory(0L);
		projection.setVirtualCores(0);

		projectedGrid.addProjection(projection);

		ContainerId id = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 0);
		Priority priority = MockUtils.getMockPriority(0);
		NodeId nodeId = MockUtils.getMockNodeId("host", 0);
		Resource resource = MockUtils.getMockResource(0, 0);
		Container container = MockUtils.getMockContainer(id, nodeId, resource, priority);
		DefaultGridMember member = new DefaultGridMember(container);
		grid.addMember(member);

		assertThat(projection, notNullValue());
		assertThat(projection.getMembers().size(), is(1));

		SatisfyStateData satisfyState = projection.getSatisfyState();
		assertThat(satisfyState.getAllocateData().getAny(), is(1));
	}

}
