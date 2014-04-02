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

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.springframework.yarn.am.allocate.DefaultAllocateCountTracker.AllocateCountInfo;

/**
 * Tests for {@link DefaultAllocateCountTracker}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultAllocateCountTrackerTests {

	@Test
	public void testOneAny() {
		DefaultAllocateCountTracker tracker = new DefaultAllocateCountTracker(new Configuration());
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addAny(1);
		tracker.addContainers(containerAllocateData);
		AllocateCountInfo allocateCounts = tracker.getAllocateCounts();
		assertThat(allocateCounts, notNullValue());
		assertThat(allocateCounts.anysInfo.size(), is(1));
		assertThat(allocateCounts.anysInfo.get("*"), is(1));
		assertThat(allocateCounts.hostsInfo.size(), is(0));
		assertThat(allocateCounts.racksInfo.size(), is(0));
	}

	@Test
	public void testOneHost() {
		DefaultAllocateCountTracker tracker = new DefaultAllocateCountTracker(new Configuration());
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addHosts("host1", 1);
		tracker.addContainers(containerAllocateData);
		AllocateCountInfo allocateCounts = tracker.getAllocateCounts();
		assertThat(allocateCounts, notNullValue());
		assertThat(allocateCounts.anysInfo.size(), is(1));
		assertThat(allocateCounts.anysInfo.get("*"), is(2));
		assertThat(allocateCounts.hostsInfo.size(), is(1));
		assertThat(allocateCounts.hostsInfo.get("host1"), is(1));
		assertThat(allocateCounts.racksInfo.size(), is(1));
		assertThat(allocateCounts.racksInfo.get("/default-rack"), is(1));
	}

	@Test
	public void testOneRack() {
		DefaultAllocateCountTracker tracker = new DefaultAllocateCountTracker(new Configuration());
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addRacks("/default-rack", 1);
		tracker.addContainers(containerAllocateData);
		AllocateCountInfo allocateCounts = tracker.getAllocateCounts();
		assertThat(allocateCounts, notNullValue());
		assertThat(allocateCounts.anysInfo.size(), is(1));
		assertThat(allocateCounts.anysInfo.get("*"), is(1));
		assertThat(allocateCounts.hostsInfo.size(), is(0));
		assertThat(allocateCounts.racksInfo.size(), is(1));
		assertThat(allocateCounts.racksInfo.get("/default-rack"), is(1));
	}

	@Test
	public void testOneHostOneRack() {
		DefaultAllocateCountTracker tracker = new DefaultAllocateCountTracker(new Configuration());
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addHosts("host1", 1);
		containerAllocateData.addRacks("/default-rack", 1);
		tracker.addContainers(containerAllocateData);
		AllocateCountInfo allocateCounts = tracker.getAllocateCounts();
		assertThat(allocateCounts, notNullValue());
		assertThat(allocateCounts.anysInfo.size(), is(1));
		assertThat(allocateCounts.anysInfo.get("*"), is(2));
		assertThat(allocateCounts.hostsInfo.size(), is(1));
		assertThat(allocateCounts.hostsInfo.get("host1"), is(1));
		assertThat(allocateCounts.racksInfo.size(), is(1));
		assertThat(allocateCounts.racksInfo.get("/default-rack"), is(1));
	}

}
