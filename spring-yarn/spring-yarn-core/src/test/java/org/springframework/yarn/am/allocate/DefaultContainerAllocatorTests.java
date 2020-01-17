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
		assertThat(createRequests.size(), is(0));
	}

	@Test
	public void testLegacyAnyCount() throws Exception {
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		allocator.setLocality(false);
		TestUtils.callMethod("internalInit", allocator);

		allocator.allocateContainers(2);

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

		assertThat(matchRequest(createRequests, "host1", 0, 1, true), notNullValue());
		assertThat(matchRequest(createRequests, "/default-rack", 0, 1, false), notNullValue());
		assertThat(matchRequest(createRequests, "*", 0, 2, false), notNullValue());
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

		assertThat(matchRequest(createRequests, "/default-rack", 0, 1, true), notNullValue());
		assertThat(matchRequest(createRequests, "*", 0, 1, false), notNullValue());
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

		assertThat(matchRequest(createRequests, "host1", 0, 1, true), notNullValue());
		assertThat(matchRequest(createRequests, "/default-rack", 0, 1, true), notNullValue());
		assertThat(matchRequest(createRequests, "*", 0, 2, true), notNullValue());
	}

	@Test
	public void testMixedHostRequests() throws Exception {
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		allocator.setAllocationValues("cluster1", 1, null, 1, 64L, false);
		allocator.setAllocationValues("cluster2", 2, null, 2, 128L, true);
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
		assertThat(createRequests.size(), is(6));

		assertResourceRequest(createRequests.get(0), "*", 2, 2, false, 128L, 2);
		assertResourceRequest(createRequests.get(1), "/default-rack", 2, 1, false, 128L, 2);
		assertResourceRequest(createRequests.get(2), "host20", 2, 1, true, 128L, 2);
		assertResourceRequest(createRequests.get(3), "*", 1, 2, true, 64L, 1);
		assertResourceRequest(createRequests.get(4), "/default-rack", 1, 1, true, 64L, 1);
		assertResourceRequest(createRequests.get(5), "host10", 1, 1, true, 64L, 1);
	}

	@Test
	public void testMixedAnyAndHostRequests() throws Exception {
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		allocator.setAllocationValues("cluster1", 1, null, 1, 64L, false);
		allocator.setAllocationValues("cluster2", 2, null, 2, 128L, true);
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
		assertThat(createRequests.size(), is(4));

		assertResourceRequest(createRequests.get(0), "*", 2, 2, false, 128L, 2);
		assertResourceRequest(createRequests.get(1), "/default-rack", 2, 1, false, 128L, 2);
		assertResourceRequest(createRequests.get(2), "host20", 2, 1, true, 128L, 2);
		assertResourceRequest(createRequests.get(3), "*", 1, 1, true, 64L, 1);
	}

	@Test
	public void testFallbackToDefaultsRequests() throws Exception {
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		TestUtils.callMethod("internalInit", allocator);

		// we don't set allocation values thus 'cluster1' is unknown
		ContainerAllocateData data1 = new ContainerAllocateData();
		data1.setId("cluster1");
		data1.addAny(1);
		allocator.allocateContainers(data1);

		List<ResourceRequest> createRequests = TestUtils.callMethod("createRequests", allocator);
		Collections.sort(createRequests, new ResourceRequestComparator());
		assertThat(createRequests, notNullValue());
		assertThat(createRequests.size(), is(1));

		assertResourceRequest(createRequests.get(0), "*", 0, 1, true, 64L, 1);
	}

	@Test
	public void testOneRackAndHostLocalityRequests() throws Exception {
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		allocator.setLocality(true);
		TestUtils.callMethod("internalInit", allocator);

		ContainerAllocateData data = new ContainerAllocateData();
		data.addRacks("/default-rack", 2);
		data.addHosts("hostX", 1);
		allocator.allocateContainers(data);

		List<ResourceRequest> createRequests = TestUtils.callMethod("createRequests", allocator);
		assertThat(createRequests, notNullValue());
		assertThat(createRequests.size(), is(5));

		assertThat(matchRequest(createRequests, "hostX", 0, 1, true), notNullValue());
		assertThat(matchRequest(createRequests, "/default-rack", 0, 1, false), notNullValue());
		assertThat(matchRequest(createRequests, "*", 0, 2, false), notNullValue());
		assertThat(matchRequest(createRequests, "/default-rack", 1, 2, true), notNullValue());
		assertThat(matchRequest(createRequests, "*", 1, 2, false), notNullValue());
	}

	@Test
	public void testAnyAndHostLocalityRequests() throws Exception {
		DefaultContainerAllocator allocator = new DefaultContainerAllocator();
		allocator.setConfiguration(new Configuration());
		allocator.setLocality(true);
		TestUtils.callMethod("internalInit", allocator);

		ContainerAllocateData data = new ContainerAllocateData();
		data.addAny(1);
		data.addHosts("host1", 1);
		allocator.allocateContainers(data);

		List<ResourceRequest> createRequests = TestUtils.callMethod("createRequests", allocator);
		assertThat(createRequests, notNullValue());
		assertThat(createRequests.size(), is(4));

		assertThat(matchRequest(createRequests, "host1", 1, 1, true), notNullValue());
		assertThat(matchRequest(createRequests, "/default-rack", 1, 1, false), notNullValue());
		assertThat(matchRequest(createRequests, "*", 1, 2, false), notNullValue());
		assertThat(matchRequest(createRequests, "*", 0, 1, true), notNullValue());
	}

	private static void assertResourceRequest(ResourceRequest req, String resourceName, Integer priority, Integer numContainers, Boolean relaxLocality, Long capMemory, Integer capCores) {
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
			assertThat(req.getCapability().getMemorySize(), is(capMemory));
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
				int x = r1.getNumContainers();
				int y = r2.getNumContainers();
				ret =  (x < y) ? -1 : ((x == y) ? 0 : 1);
			}
			if (ret == 0) {
				boolean x = r1.getRelaxLocality();
				boolean y = r2.getRelaxLocality();
				ret = (x == y) ? 0 : (x ? 1 : -1);
			}
			return ret;
		}
	}

	private static ResourceRequest matchRequest(List<ResourceRequest> createRequests, String name, int priority,
			int containers, boolean relax) {
		for (ResourceRequest r : createRequests) {
			if (r.getResourceName().equals(name) && r.getPriority().getPriority() == priority
					&& r.getNumContainers() == containers && r.getRelaxLocality() == relax) {
				return r;
			}
		}
		return null;
	}

}
