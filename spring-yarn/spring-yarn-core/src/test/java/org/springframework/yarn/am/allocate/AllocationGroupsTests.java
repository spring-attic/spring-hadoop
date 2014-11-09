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
package org.springframework.yarn.am.allocate;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * Tests for {@link AllocationGroups}.
 *
 * @author Janne Valkealahti
 *
 */
public class AllocationGroupsTests {

	@Test
	public void testDefaults() {
		AllocationGroups groups = new AllocationGroups();
		assertThat(groups.getAllocateCountTrackers().size(), is(0));
	}

	@Test
	public void testGroupReservation() {
		AllocationGroups groups = new AllocationGroups();
		AllocationGroup group = groups.add("g1", 0);
		assertThat(group, notNullValue());
		assertThat(group.getBasePriority(), is(0));
	}

	@Test
	public void testGroupReservedPriorities() {
		AllocationGroups groups = new AllocationGroups();
		AllocationGroup g1 = groups.add("g1", 0);
		groups.reserve("g1", "any");
		groups.reserve("g1", "host");
		assertThat(g1.getPriority("any"), is(0));
		assertThat(g1.getPriority("host"), is(1));
	}

	@Test
	public void testNextFreeForGroup() {
		AllocationGroups groups = new AllocationGroups();

		AllocationGroup group1 = groups.add("g1", 0);
		assertThat(group1, notNullValue());
		assertThat(group1.getBasePriority(), is(0));
		groups.reserve("g1", "sub1");
		assertThat(group1.getPriority("sub1"), is(0));

		AllocationGroup group2 = groups.add("g2", 0);
		assertThat(group2, notNullValue());
		assertThat(group2.getBasePriority(), is(0));
		groups.reserve("g2", "sub1");
		assertThat(group2.getPriority("sub1"), is(1));
	}

	@Test
	public void testNextFreeForGroup2() {
		AllocationGroups groups = new AllocationGroups();

		AllocationGroup group1 = groups.add("g1", 0);
		assertThat(group1, notNullValue());
		assertThat(group1.getBasePriority(), is(0));
		groups.reserve("g1", "sub1");
		assertThat(group1.getPriority("sub1"), is(0));

		AllocationGroup group2 = groups.add("g2", 10);
		assertThat(group2, notNullValue());
		assertThat(group2.getBasePriority(), is(10));
		groups.reserve("g2", "sub1");
		assertThat(group2.getPriority("sub1"), is(10));
	}

}
