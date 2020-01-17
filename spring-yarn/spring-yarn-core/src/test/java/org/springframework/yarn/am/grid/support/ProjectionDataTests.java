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
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * Tests for {@link ProjectionData}.
 *
 * @author Janne Valkealahti
 *
 */
public class ProjectionDataTests {

	@Test
	public void testSetGet() {
		ProjectionData other = new ProjectionData();
		other.setType("type");
		other.setPriority(10);
		other.setMemory(20L);
		other.setVirtualCores(5);
		other.setLocality(true);
		other.setAny(3);
		other.setHost("host", 2);
		other.setRack("rack", 3);

		assertThat(other.getType(), is("type"));
		assertThat(other.getPriority(), is(10));
		assertThat(other.getMemory(), is(20L));
		assertThat(other.getVirtualCores(), is(5));
		assertThat(other.getLocality(), is(true));
		assertThat(other.getAny(), is(3));
		assertThat(other.getHosts().size(), is(1));
		assertThat(other.getHosts().get("host"), is(2));
		assertThat(other.getRacks().size(), is(1));
		assertThat(other.getRacks().get("rack"), is(3));
	}

	@Test
	public void testMerging() {
		ProjectionData data = new ProjectionData();
		ProjectionData other = new ProjectionData();
		other.setType("type");
		other.setPriority(10);
		other.setMemory(20L);
		other.setVirtualCores(5);
		other.setLocality(true);
		other.setAny(3);
		other.setHost("host", 2);
		other.setRack("rack", 3);
		ProjectionData merged = data.merge(other);

		assertThat(merged.getType(), is("type"));
		assertThat(merged.getPriority(), is(10));
		assertThat(merged.getMemory(), is(20L));
		assertThat(merged.getVirtualCores(), is(5));
		assertThat(merged.getLocality(), is(true));
		assertThat(merged.getAny(), is(3));
		assertThat(merged.getHosts().size(), is(1));
		assertThat(merged.getHosts().get("host"), is(2));
		assertThat(merged.getRacks().size(), is(1));
		assertThat(merged.getRacks().get("rack"), is(3));
	}

}
