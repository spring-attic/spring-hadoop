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

import java.io.Serializable;
import java.util.Collections;
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
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		TestUtils.callMethod("internalInit", allocator);

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
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		allocator.setLocality(false);
		TestUtils.callMethod("internalInit", allocator);

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
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		allocator.setLocality(true);
		TestUtils.callMethod("internalInit", allocator);

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
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		allocator.setLocality(true);
		TestUtils.callMethod("internalInit", allocator);

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
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		allocator.setLocality(false);
		TestUtils.callMethod("internalInit", allocator);

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

	@Test
	public void testMixedHostRequests() throws Exception {
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		allocator.setAllocationValues("cluster1", 1, 1, 64, false);
		allocator.setAllocationValues("cluster2", 2, 2, 128, true);
		TestUtils.callMethod("internalInit", allocator);

		ContainerAllocateData data1 = new ContainerAllocateData();
		data1.setId("cluster1");
		data1.addHosts("host10", 1);
		allocator.allocateContainers(data1);

		ContainerAllocateData data2 = new ContainerAllocateData();
		data2.setId("cluster2");
		data2.addHosts("host20", 1);
		allocator.allocateContainers(data2);

		List<ResourceRequest> createRequests = TestUtils.callMethod("createRequests", allocator);
		Collections.sort(createRequests, new ResourceRequestComparator());
		assertThat(createRequests, notNullValue());
		assertThat(createRequests.size(), is(7));

		assertResourceRequest(createRequests.get(0), "*", 2, 2, false, 128, 2);
		assertResourceRequest(createRequests.get(1), "/default-rack", 2, 1, false, 128, 2);
		assertResourceRequest(createRequests.get(2), "host20", 2, 1, true, 128, 2);
		assertResourceRequest(createRequests.get(3), "*", 1, 2, true, 64, 1);
		assertResourceRequest(createRequests.get(4), "/default-rack", 1, 1, true, 64, 1);
		assertResourceRequest(createRequests.get(5), "host10", 1, 1, true, 64, 1);
		assertResourceRequest(createRequests.get(6), "*", 0, 0, true, 64, 1);
	}

	@Test
	public void testMixedAnyAndHostRequests() throws Exception {
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		allocator.setAllocationValues("cluster1", 1, 1, 64, false);
		allocator.setAllocationValues("cluster2", 2, 2, 128, true);
		TestUtils.callMethod("internalInit", allocator);

		ContainerAllocateData data1 = new ContainerAllocateData();
		data1.setId("cluster1");
		data1.addAny(1);
		allocator.allocateContainers(data1);

		ContainerAllocateData data2 = new ContainerAllocateData();
		data2.setId("cluster2");
		data2.addHosts("host20", 1);
		allocator.allocateContainers(data2);

		List<ResourceRequest> createRequests = TestUtils.callMethod("createRequests", allocator);
		Collections.sort(createRequests, new ResourceRequestComparator());
		assertThat(createRequests, notNullValue());
		assertThat(createRequests.size(), is(5));

		assertResourceRequest(createRequests.get(0), "*", 2, 2, false, 128, 2);
		assertResourceRequest(createRequests.get(1), "/default-rack", 2, 1, false, 128, 2);
		assertResourceRequest(createRequests.get(2), "host20", 2, 1, true, 128, 2);
		assertResourceRequest(createRequests.get(3), "*", 1, 1, true, 64, 1);
		assertResourceRequest(createRequests.get(4), "*", 0, 0, true, 64, 1);
	}

	@Test
	public void testFallbackToDefaultsRequests() throws Exception {
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		TestUtils.callMethod("internalInit", allocator);

		ContainerAllocateData data1 = new ContainerAllocateData();
		data1.setId("cluster1");
		data1.addAny(1);
		allocator.allocateContainers(data1);

		List<ResourceRequest> createRequests = TestUtils.callMethod("createRequests", allocator);
		Collections.sort(createRequests, new ResourceRequestComparator());
		assertThat(createRequests, notNullValue());
		assertThat(createRequests.size(), is(1));

		assertResourceRequest(createRequests.get(0), "*", 0, 1, true, 64, 1);
	}

	private static void assertResourceRequest(ResourceRequest req, String resourceName, Integer priority, Integer numContainers, Boolean relaxLocality, Integer capMemory, Integer capCores) {
		if (resourceName != null) {
			assertThat(req.getResourceName(), is(resourceName));
		}
		if (priority != null) {
			assertThat(req.getPriority().getPriority(), is(priority));
		}
		if (numContainers != null) {
			assertThat(req.getNumContainers(), is(numContainers));
		}
		if (relaxLocality != null) {
			assertThat(req.getRelaxLocality(), is(relaxLocality));
		}
		if (capMemory != null) {
			assertThat(req.getCapability().getMemory(), is(capMemory));
		}
		if (capCores != null) {
			assertThat(req.getCapability().getVirtualCores(), is(capCores));
		}
	}

	/**
	 * Comparator used to sort requests so we get expected ordering for asserts.
	 */
	public static class ResourceRequestComparator implements java.util.Comparator<ResourceRequest>, Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public int compare(ResourceRequest r1, ResourceRequest r2) {

			// Compare priority, host and capability
			int ret = r1.getPriority().compareTo(r2.getPriority());
			if (ret == 0) {
				String h1 = r1.getResourceName();
				String h2 = r2.getResourceName();
				ret = h1.compareTo(h2);
			}
			if (ret == 0) {
				ret = r1.getCapability().compareTo(r2.getCapability());
			}
			if (ret == 0) {
				ret = Integer.compare(r1.getNumContainers(), r2.getNumContainers());
			}
			if (ret == 0) {
				Boolean.compare(r1.getRelaxLocality(), r2.getRelaxLocality());
			}
			return ret;
		}
	}

}
