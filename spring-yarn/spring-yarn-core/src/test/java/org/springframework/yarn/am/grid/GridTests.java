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
package org.springframework.yarn.am.grid;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.junit.Test;
import org.springframework.yarn.MockUtils;
import org.springframework.yarn.am.grid.support.DefaultGrid;
import org.springframework.yarn.am.grid.support.DefaultGridMember;

public class GridTests {

	@Test
	public void testSimpleOperations() {
		DefaultGrid grid = new DefaultGrid();
		ContainerId id = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 0);
		Container container = MockUtils.getMockContainer(id, null, null, null);
		DefaultGridMember member = new DefaultGridMember(container);
		grid.addMember(member);
		assertThat(grid.getMembers().size(), is(1));
	}


}
