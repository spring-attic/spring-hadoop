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
import org.apache.hadoop.yarn.api.records.Priority;
import org.junit.Test;
import org.springframework.yarn.MockUtils;
import org.springframework.yarn.TestUtils;
import org.springframework.yarn.am.grid.GridProjection;

public class AnyGridProjectionTests {

	@Test
	public void testDefaults() {
		GridProjection projection = new AnyGridProjection();
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
	public void testAddOne() {
		GridProjection projection = new AnyGridProjection();
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
	public void testAddOneAddMember() {
		AnyGridProjection projection = new AnyGridProjection();
		projection.setProjectionData(new ProjectionData(1));
		projection.setPriority(0);

		ContainerId id = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 0);
		Priority priority = MockUtils.getMockPriority(0);
		Container container = MockUtils.getMockContainer(id, null, null, priority);
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
	public void testAddTwoAddMembersRemoveMember() throws Exception {
		AnyGridProjection projection = new AnyGridProjection();
		projection.setPriority(0);
		projection.setProjectionData(new ProjectionData(2));
		Integer count = TestUtils.readField("count", projection);
		assertThat(count, is(2));

		Priority priority = MockUtils.getMockPriority(0);

		ContainerId id1 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 1);
		Container container1 = MockUtils.getMockContainer(id1, null, null, priority);
		DefaultGridMember member1 = new DefaultGridMember(container1);
		projection.acceptMember(member1);

		ContainerId id2 = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 2);
		Container container2 = MockUtils.getMockContainer(id2, null, null, priority);
		DefaultGridMember member2 = new DefaultGridMember(container2);
		projection.acceptMember(member2);

		SatisfyStateData satisfyState = projection.getSatisfyState();
		assertThat(satisfyState, notNullValue());
		assertThat(satisfyState.getAllocateData(), notNullValue());
		assertThat(satisfyState.getAllocateData().getAny(), is(0));
		assertThat(satisfyState.getRemoveData().size(), is(0));

		projection.setProjectionData(new ProjectionData(1));
		count = TestUtils.readField("count", projection);
		assertThat(count, is(1));
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

}
