/*
 * Copyright 2013 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.util.Assert;
import org.springframework.yarn.listener.CompositeContainerAllocatorListener;
import org.springframework.yarn.listener.ContainerAllocatorListener;
import org.springframework.yarn.support.compat.ResourceCompat;

/**
 * Default allocator which polls resource manager, requests new containers
 * and acts as a heart beat sender at the same time.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultContainerAllocator extends AbstractPollingAllocator implements ContainerAllocator {

	private static final Log log = LogFactory.getLog(DefaultContainerAllocator.class);

	/** Listener dispatcher for events */
	private CompositeContainerAllocatorListener allocatorListener = new CompositeContainerAllocatorListener();

	/** Container request priority */
	private int priority = 0;

	/** Default hosts to schedule */
	private String[] hosts = new String[0];

	/** Default racks to schedule */
	private String[] racks = new String[0];

	/** Resource capability as of cores */
	private int virtualcores = 1;

	/** Resource capability as of memory */
	private int memory = 64;

	/** Increasing counter for rpc request id*/
	private AtomicInteger requestId = new AtomicInteger();

	/** Current progress reported during allocate requests */
	private float applicationProgress = 0;

	/** Queued container id's to be released */
	private Queue<ContainerId> releaseContainers = new ConcurrentLinkedQueue<ContainerId>();

	/** Tracker for request counts */
	private DefaultAllocateCountTracker allocateCountTracker = new DefaultAllocateCountTracker();

	@Override
	public void allocateContainers(int count) {
		if(log.isDebugEnabled()) {
			log.debug("Incoming count: " + count);
		}
		allocateCountTracker.addContainers(count);
	}

	@Override
	public void addListener(ContainerAllocatorListener listener) {
		allocatorListener.register(listener);
	}

	@Override
	public void allocateContainers(ContainerAllocateData containerAllocateData) {
		if(log.isDebugEnabled()) {
			log.debug("Incoming containerAllocateData: " + containerAllocateData);
		}

		if(log.isDebugEnabled()) {
			log.debug("State allocateCountTracker before adding allocation data: " + allocateCountTracker);
		}

		allocateCountTracker.addContainers(containerAllocateData);

		if(log.isDebugEnabled()) {
			log.debug("State allocateCountTracker after adding allocation data: " + allocateCountTracker);
		}
	}

	@Override
	public void releaseContainers(List<Container> containers) {
		for (Container container : containers) {
			releaseContainer(container.getId());
		}
	}

	@Override
	public void releaseContainer(ContainerId containerId) {
		releaseContainers.add(containerId);
	}

	@Override
	protected AMResponse doContainerRequest() {
		List<ResourceRequest> requestedContainers = new ArrayList<ResourceRequest>();
		Map<String, Integer> allocateCounts = allocateCountTracker.getAllocateCounts();

		for (Entry<String, Integer> entry : allocateCounts.entrySet()) {
			requestedContainers.add(getContainerResourceRequest(entry.getValue(), entry.getKey()));
		}

		// add pending containers to be released
		List<ContainerId> release = new ArrayList<ContainerId>();
		ContainerId element = null;
		while ((element = releaseContainers.poll()) != null) {
			release.add(element);
		}

		if(log.isDebugEnabled()) {
			log.debug("Requesting containers using " + requestedContainers.size() + " requests.");
			for (ResourceRequest resourceRequest : requestedContainers) {
				log.debug("ResourceRequest: " + resourceRequest + " with count=" +
						resourceRequest.getNumContainers() + " with hostName=" + resourceRequest.getHostName());
			}
			log.debug("Request id will be: " + requestId.get());
		}

		// build the allocation request
		AllocateRequest request = Records.newRecord(AllocateRequest.class);
		request.setResponseId(requestId.get());
		request.setApplicationAttemptId(getApplicationAttemptId());
		request.addAllAsks(requestedContainers);
		request.addAllReleases(release);
		request.setProgress(applicationProgress);

		// do request and return response
		AllocateResponse allocate = getRmTemplate().allocate(request);
		requestId.set(allocate.getAMResponse().getResponseId());
		return allocate.getAMResponse();
	}

	@Override
	protected List<Container> preProcessAllocatedContainers(List<Container> containers) {
		// checking if we have standing allocation counts,
		// if not assume as garbage and send to release queue.
		// what's left will be out of our hand and expected to be
		// processed by the listener and eventually send back
		// to us as a released container.

		if(log.isDebugEnabled()) {
			log.debug("State allocateCountTracker before handling allocated container: " + allocateCountTracker);
		}

		List<Container> preProcessed = new ArrayList<Container>();
		for (Container container : containers) {
			Container processed = allocateCountTracker.processAllocatedContainer(container);
			if (processed != null) {
				preProcessed.add(processed);
			} else {
				releaseContainers.add(container.getId());
			}
		}

		if(log.isDebugEnabled()) {
			log.debug("State allocateCountTracker after handling allocated container: " + allocateCountTracker);
		}

		return preProcessed;
	}

	@Override
	protected void handleAllocatedContainers(List<Container> containers) {
		allocatorListener.allocated(containers);
	}

	@Override
	protected void handleCompletedContainers(List<ContainerStatus> containerStatuses) {
		allocatorListener.completed(containerStatuses);
	}

	@Override
	public void setProgress(float progress) {
		applicationProgress = progress;
	}

	/**
	 * Gets the priority for container request.
	 *
	 * @return the priority
	 */
	public int getPriority() {
		return priority;
	}

	/**
	 * Sets the priority for container request.
	 *
	 * @param priority the new priority
	 */
	public void setPriority(int priority) {
		this.priority = priority;
	}

	/**
	 * Gets the hosts.
	 *
	 * @return the hosts
	 */
	public String[] getHosts() {
		return hosts;
	}

	/**
	 * Sets the hosts.
	 *
	 * @param hosts the new hosts
	 */
	public void setHosts(String[] hosts) {
		Assert.notNull(hosts, "Hosts array must not be null");
		this.hosts = hosts;
	}

	/**
	 * Gets the racks.
	 *
	 * @return the racks
	 */
	public String[] getRacks() {
		return racks;
	}

	/**
	 * Sets the racks.
	 *
	 * @param racks the new racks
	 */
	public void setRacks(String[] racks) {
		Assert.notNull(racks, "Racks array must not be null");
		this.racks = racks;
	}

	/**
	 * Gets the virtualcores for container request.
	 *
	 * @return the virtualcores
	 */
	public int getVirtualcores() {
		return virtualcores;
	}

	/**
	 * Sets the virtualcores for container request defining
	 * <em>number of virtual cpu cores</em> of the resource.
	 *
	 * @param virtualcores the new virtualcores
	 */
	public void setVirtualcores(int virtualcores) {
		this.virtualcores = virtualcores;
	}

	/**
	 * Gets the memory for container request.
	 *
	 * @return the memory
	 */
	public int getMemory() {
		return memory;
	}

	/**
	 * Sets the memory for container request defining
	 * <em>memory</em> of the resource.
	 *
	 * @param memory the new memory
	 */
	public void setMemory(int memory) {
		this.memory = memory;
	}

	/**
	 * Utility method creating a {@link ResourceRequest}.
	 *
	 * @param numContainers number of containers to request
	 * @return request to be sent to resource manager
	 */
	private ResourceRequest getContainerResourceRequest(int numContainers, String hostName) {
		ResourceRequest request = Records.newRecord(ResourceRequest.class);
		request.setHostName(hostName);
		request.setNumContainers(numContainers);
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(priority);
		request.setPriority(pri);
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(memory);
		ResourceCompat.setVirtualCores(capability, virtualcores);
		request.setCapability(capability);
		return request;
	}

}
