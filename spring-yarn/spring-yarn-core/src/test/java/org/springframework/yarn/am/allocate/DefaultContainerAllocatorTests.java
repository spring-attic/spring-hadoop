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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.junit.Test;
import org.springframework.yarn.TestUtils;

/**
 * Tests for {@link DefaultContainerAllocator}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultContainerAllocatorTests {

	@Test
	public void testDefaultNoRequests() throws Exception {
		DefaultAllocateCountTracker allocateCountTracker = new DefaultAllocateCountTracker(new Configuration());
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		TestUtils.setField("allocateCountTracker", allocator, allocateCountTracker);

		List<ResourceRequest> createRequests = TestUtils.callMethod("createRequests", allocator);
		assertThat(createRequests, notNullValue());
		assertThat(createRequests.size(), is(1));
		ResourceRequest req = createRequests.get(0);
		assertThat(req.getPriority().getPriority(), is(0));
		assertThat(req.getNumContainers(), is(0));
		assertThat(req.getRelaxLocality(), is(true));
	}

	@Test
	public void testAnyHostNoLocalityRequests() throws Exception {
		DefaultAllocateCountTracker allocateCountTracker = new DefaultAllocateCountTracker(new Configuration());
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setLocality(false);
		TestUtils.setField("allocateCountTracker", allocator, allocateCountTracker);

		ContainerAllocateData data = new ContainerAllocateData();
		data.addAny(2);
		allocator.allocateContainers(data);

		List<ResourceRequest> createRequests = TestUtils.callMethod("createRequests", allocator);
		assertThat(createRequests, notNullValue());
		assertThat(createRequests.size(), is(1));

		ResourceRequest req0 = createRequests.get(0);
		assertThat(req0.getResourceName(), is("*"));
		assertThat(req0.getPriority().getPriority(), is(0));
		assertThat(req0.getNumContainers(), is(2));
		assertThat(req0.getRelaxLocality(), is(true));
	}

	@Test
	public void testOneHostLocalityRequests() throws Exception {
		DefaultAllocateCountTracker allocateCountTracker = new DefaultAllocateCountTracker(new Configuration());
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setLocality(true);
		TestUtils.setField("allocateCountTracker", allocator, allocateCountTracker);

		ContainerAllocateData data = new ContainerAllocateData();
		data.addHosts("host1", 1);
		allocator.allocateContainers(data);

		List<ResourceRequest> createRequests = TestUtils.callMethod("createRequests", allocator);
		assertThat(createRequests, notNullValue());
		assertThat(createRequests.size(), is(3));

		ResourceRequest req0 = createRequests.get(0);
		assertThat(req0.getResourceName(), is("host1"));
		assertThat(req0.getPriority().getPriority(), is(0));
		assertThat(req0.getNumContainers(), is(1));
		assertThat(req0.getRelaxLocality(), is(true));

		ResourceRequest req1 = createRequests.get(1);
		assertThat(req1.getResourceName(), is("/default-rack"));
		assertThat(req1.getPriority().getPriority(), is(0));
		assertThat(req1.getNumContainers(), is(1));
		assertThat(req1.getRelaxLocality(), is(false));

		ResourceRequest req2 = createRequests.get(2);
		assertThat(req2.getResourceName(), is("*"));
		assertThat(req2.getPriority().getPriority(), is(0));
		assertThat(req2.getNumContainers(), is(2));
		assertThat(req2.getRelaxLocality(), is(false));
	}

	@Test
	public void testOneRackLocalityRequests() throws Exception {
		DefaultAllocateCountTracker allocateCountTracker = new DefaultAllocateCountTracker(new Configuration());
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setLocality(true);
		TestUtils.setField("allocateCountTracker", allocator, allocateCountTracker);

		ContainerAllocateData data = new ContainerAllocateData();
		data.addRacks("/default-rack", 1);
		allocator.allocateContainers(data);

		List<ResourceRequest> createRequests = TestUtils.callMethod("createRequests", allocator);
		assertThat(createRequests, notNullValue());
		assertThat(createRequests.size(), is(2));

		ResourceRequest req0 = createRequests.get(0);
		assertThat(req0.getResourceName(), is("/default-rack"));
		assertThat(req0.getPriority().getPriority(), is(0));
		assertThat(req0.getNumContainers(), is(1));
		assertThat(req0.getRelaxLocality(), is(true));

		ResourceRequest req1 = createRequests.get(1);
		assertThat(req1.getResourceName(), is("*"));
		assertThat(req1.getPriority().getPriority(), is(0));
		assertThat(req1.getNumContainers(), is(1));
		assertThat(req1.getRelaxLocality(), is(false));
	}

	@Test
	public void testOneHostNoLocalityRequests() throws Exception {
		DefaultAllocateCountTracker allocateCountTracker = new DefaultAllocateCountTracker(new Configuration());
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setLocality(false);
		TestUtils.setField("allocateCountTracker", allocator, allocateCountTracker);

		ContainerAllocateData data = new ContainerAllocateData();
		data.addHosts("host1", 1);
		allocator.allocateContainers(data);

		List<ResourceRequest> createRequests = TestUtils.callMethod("createRequests", allocator);
		assertThat(createRequests, notNullValue());
		assertThat(createRequests.size(), is(3));

		ResourceRequest req0 = createRequests.get(0);
		assertThat(req0.getResourceName(), is("host1"));
		assertThat(req0.getPriority().getPriority(), is(0));
		assertThat(req0.getNumContainers(), is(1));
		assertThat(req0.getRelaxLocality(), is(true));

		ResourceRequest req1 = createRequests.get(1);
		assertThat(req1.getResourceName(), is("/default-rack"));
		assertThat(req1.getPriority().getPriority(), is(0));
		assertThat(req1.getNumContainers(), is(1));
		assertThat(req1.getRelaxLocality(), is(true));

		ResourceRequest req2 = createRequests.get(2);
		assertThat(req2.getResourceName(), is("*"));
		assertThat(req2.getPriority().getPriority(), is(0));
		assertThat(req2.getNumContainers(), is(2));
		assertThat(req2.getRelaxLocality(), is(true));
	}

}
