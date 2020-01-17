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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.RackResolver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.yarn.MockUtils;
import org.springframework.yarn.TestUtils;
import org.springframework.yarn.am.allocate.DefaultAllocateCountTracker.AllocateCountInfo;

/**
 * Tests for {@link DefaultAllocateCountTracker}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultAllocateCountTrackerTests {

	@Before
	public void setup() throws Exception {
		// try to fix some hadoop static init usage
		// do it again after a test
		TestUtils.setField("initCalled", RackResolver.class, false);
	}

	@After
	public void clean() throws Exception {
		TestUtils.setField("initCalled", RackResolver.class, false);
	}
	
	@Test
	public void testOneAny() {
		DefaultAllocateCountTracker tracker = getTracker();
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
		DefaultAllocateCountTracker tracker = getTracker();
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addHosts("hostX", 1);
		tracker.addContainers(containerAllocateData);
		AllocateCountInfo allocateCounts = tracker.getAllocateCounts();
		assertThat(allocateCounts, notNullValue());
		assertThat(allocateCounts.anysInfo.size(), is(1));
		assertThat(allocateCounts.anysInfo.get("*"), is(2));
		assertThat(allocateCounts.hostsInfo.size(), is(1));
		assertThat(allocateCounts.hostsInfo.get("hostX"), is(1));
		assertThat(allocateCounts.racksInfo.size(), is(1));
		assertThat(allocateCounts.racksInfo.get("/default-rack"), is(1));
	}

	@Test
	public void testOneRack() {
		DefaultAllocateCountTracker tracker = getTracker();
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
		DefaultAllocateCountTracker tracker = getTracker();
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addHosts("hostX", 1);
		containerAllocateData.addRacks("/default-rack", 1);
		tracker.addContainers(containerAllocateData);
		AllocateCountInfo allocateCounts = tracker.getAllocateCounts();
		assertThat(allocateCounts, notNullValue());
		assertThat(allocateCounts.anysInfo.size(), is(1));
		assertThat(allocateCounts.anysInfo.get("*"), is(3));
		assertThat(allocateCounts.hostsInfo.size(), is(1));
		assertThat(allocateCounts.hostsInfo.get("hostX"), is(1));
		assertThat(allocateCounts.racksInfo.size(), is(1));
		assertThat(allocateCounts.racksInfo.get("/default-rack"), is(2));
	}

	@Test
	public void testMultiAddHosts() {
		DefaultAllocateCountTracker tracker = getTracker();
		
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addHosts("host1", 1);
		tracker.addContainers(containerAllocateData);

		containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addHosts("host1", 2);
		tracker.addContainers(containerAllocateData);
		
		AllocateCountInfo allocateCounts = tracker.getAllocateCounts();
		assertThat(allocateCounts, notNullValue());
		assertThat(allocateCounts.anysInfo.size(), is(1));
		assertThat(allocateCounts.anysInfo.get("*"), is(6));
		assertThat(allocateCounts.hostsInfo.size(), is(1));
		assertThat(allocateCounts.hostsInfo.get("host1"), is(3));
		assertThat(allocateCounts.racksInfo.size(), is(1));
		assertThat(allocateCounts.racksInfo.get("/rack1"), is(3));
	}

	@Test
	public void testMultiHostsAndRacks() {
		DefaultAllocateCountTracker tracker = getTracker();
		
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addHosts("host1", 1);
		tracker.addContainers(containerAllocateData);

		containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addHosts("host1", 2);
		tracker.addContainers(containerAllocateData);

		containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addRacks("/rack1", 1);
		tracker.addContainers(containerAllocateData);
		
		AllocateCountInfo allocateCounts = tracker.getAllocateCounts();
		assertThat(allocateCounts, notNullValue());
		assertThat(allocateCounts.anysInfo.size(), is(1));
		assertThat(allocateCounts.anysInfo.get("*"), is(7));
		assertThat(allocateCounts.hostsInfo.size(), is(1));
		assertThat(allocateCounts.hostsInfo.get("host1"), is(3));
		assertThat(allocateCounts.racksInfo.size(), is(1));
		assertThat(allocateCounts.racksInfo.get("/rack1"), is(4));
	}

	@Test
	public void testHostsAndRacksInSameData() {
		DefaultAllocateCountTracker tracker = getTracker();
		
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addHosts("host1", 1);
		containerAllocateData.addHosts("host1", 2);
		containerAllocateData.addRacks("/rack1", 1);
		tracker.addContainers(containerAllocateData);
		
		AllocateCountInfo allocateCounts = tracker.getAllocateCounts();
		assertThat(allocateCounts, notNullValue());
		assertThat(allocateCounts.anysInfo.size(), is(1));
		assertThat(allocateCounts.anysInfo.get("*"), is(7));
		assertThat(allocateCounts.hostsInfo.size(), is(1));
		assertThat(allocateCounts.hostsInfo.get("host1"), is(3));
		assertThat(allocateCounts.racksInfo.size(), is(1));
		assertThat(allocateCounts.racksInfo.get("/rack1"), is(4));
	}
	
	@Test
	public void testOneAnyProcessing() throws Exception {
		DefaultAllocateCountTracker tracker = getTracker();
		
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addAny(1);
		tracker.addContainers(containerAllocateData);
		
		Container container = getMockContainer("hostX");

		AtomicInteger pendingAny = TestUtils.readField("pendingAny", tracker);
		AtomicInteger requestedAny = TestUtils.readField("requestedAny", tracker);
		assertThat(pendingAny.get(), is(1));
		assertThat(requestedAny.get(), is(0));
		
		tracker.getAllocateCounts();
		pendingAny = TestUtils.readField("pendingAny", tracker);
		requestedAny = TestUtils.readField("requestedAny", tracker);
		assertThat(pendingAny.get(), is(0));
		assertThat(requestedAny.get(), is(1));
		
		tracker.processAllocatedContainer(container);
		pendingAny = TestUtils.readField("pendingAny", tracker);
		requestedAny = TestUtils.readField("requestedAny", tracker);
		assertThat(pendingAny.get(), is(0));
		assertThat(requestedAny.get(), is(0));
	}
	
	@Test
	public void testOneRackProcessing() throws Exception {
		DefaultAllocateCountTracker tracker = getTracker();
		
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addRacks("/default-rack", 1);
		tracker.addContainers(containerAllocateData);
		
		Map<String, AtomicInteger> pendingRacks = TestUtils.readField("pendingRacks", tracker);
		Map<String, AtomicInteger> requestedRacks = TestUtils.readField("requestedRacks", tracker);
		assertThat(pendingRacks.get("/default-rack").get(), is(1));
		assertThat(requestedRacks.get("/default-rack"), nullValue());

		Container container = getMockContainer("hostX");
		
		tracker.getAllocateCounts();
		pendingRacks = TestUtils.readField("pendingRacks", tracker);
		requestedRacks = TestUtils.readField("requestedRacks", tracker);
		assertThat(pendingRacks.get("/default-rack").get(), is(0));
		assertThat(requestedRacks.get("/default-rack").get(), is(1));

		tracker.processAllocatedContainer(container);
		pendingRacks = TestUtils.readField("pendingRacks", tracker);
		requestedRacks = TestUtils.readField("requestedRacks", tracker);
		assertThat(pendingRacks.get("/default-rack").get(), is(0));
		assertThat(requestedRacks.get("/default-rack").get(), is(0));
	}
	
	@Test
	public void testMixedHostRackProcessing() throws Exception {
		DefaultAllocateCountTracker tracker = getTracker();
		
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		containerAllocateData.addRacks("/rack1", 1);
		containerAllocateData.addHosts("host2", 1);
		tracker.addContainers(containerAllocateData);

		Map<String, AtomicInteger> pendingRacks = TestUtils.readField("pendingRacks", tracker);
		Map<String, AtomicInteger> requestedRacks = TestUtils.readField("requestedRacks", tracker);
		Map<String, AtomicInteger> pendingHosts = TestUtils.readField("pendingHosts", tracker);
		Map<String, AtomicInteger> requestedHosts = TestUtils.readField("requestedHosts", tracker);
		assertThat(pendingRacks.get("/rack1").get(), is(1));
		assertThat(requestedRacks.get("/rack1"), nullValue());
		assertThat(pendingHosts.get("host2").get(), is(1));
		assertThat(requestedHosts.get("host2"), nullValue());

		tracker.getAllocateCounts();
		pendingRacks = TestUtils.readField("pendingRacks", tracker);
		requestedRacks = TestUtils.readField("requestedRacks", tracker);
		pendingHosts = TestUtils.readField("pendingHosts", tracker);
		requestedHosts = TestUtils.readField("requestedHosts", tracker);
		assertThat(pendingRacks.get("/rack1").get(), is(0));
		assertThat(requestedRacks.get("/rack1").get(), is(1));
		assertThat(pendingHosts.get("host2").get(), is(0));
		assertThat(requestedHosts.get("host2").get(), is(1));

		Container container = getMockContainer("host2");
		tracker.processAllocatedContainer(container);
		pendingRacks = TestUtils.readField("pendingRacks", tracker);
		requestedRacks = TestUtils.readField("requestedRacks", tracker);
		pendingHosts = TestUtils.readField("pendingHosts", tracker);
		requestedHosts = TestUtils.readField("requestedHosts", tracker);
		assertThat(pendingRacks.get("/rack1").get(), is(0));
		assertThat(requestedRacks.get("/rack1").get(), is(1));
		assertThat(pendingHosts.get("host2").get(), is(0));
		assertThat(requestedHosts.get("host2").get(), is(0));
		
		container = getMockContainer("host1");
		tracker.processAllocatedContainer(container);
		pendingRacks = TestUtils.readField("pendingRacks", tracker);
		requestedRacks = TestUtils.readField("requestedRacks", tracker);
		pendingHosts = TestUtils.readField("pendingHosts", tracker);
		requestedHosts = TestUtils.readField("requestedHosts", tracker);
		assertThat(pendingRacks.get("/rack1").get(), is(0));
		assertThat(requestedRacks.get("/rack1").get(), is(0));
		assertThat(pendingHosts.get("host2").get(), is(0));
		assertThat(requestedHosts.get("host2").get(), is(0));
	}
	
	private static Container getMockContainer(String host) {
		ContainerId containerId = MockUtils.getMockContainerId(MockUtils.getMockApplicationAttemptId(0, 0), 0);
		NodeId nodeId = MockUtils.getMockNodeId(host, 0);
		Priority priority = MockUtils.getMockPriority(0);
		Resource resource = MockUtils.getMockResource(0, 0);
		return MockUtils.getMockContainer(containerId, nodeId, resource, priority);		
	}
	
	private DefaultAllocateCountTracker getTracker() {
		Configuration configuration = new Configuration();
		configuration.set("net.topology.node.switch.mapping.impl", "org.springframework.yarn.am.grid.support.TestDNSToSwitchMapping");
		return new DefaultAllocateCountTracker(configuration);
	}

}
