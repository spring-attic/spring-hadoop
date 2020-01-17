/*
 * Copyright 2014-2016 the original author or authors.
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
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.allocate.AllocationGroup.ContainerAllocationValues;
import org.springframework.yarn.am.allocate.DefaultAllocateCountTracker.AllocateCountInfo;
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

	/** Container label expression */
	private String labelExpression;

	/** Resource capability as of cores */
	private int virtualcores = 1;

	/** Resource capability as of memory */
	private long memory = 64;

	/** Locality relaxing */
	private boolean locality = false;

	/** Increasing counter for rpc request id*/
	private AtomicInteger requestId = new AtomicInteger();

	/** Current progress reported during allocate requests */
	private float applicationProgress = 0;

	/** Queued container id's to be released */
	private Queue<ContainerId> releaseContainers = new ConcurrentLinkedQueue<ContainerId>();

	/** Internal set of containers marked as garbage by allocate tracker */
	private Set<ContainerId> garbageContainers = new HashSet<ContainerId>();

	/** Flag helping to avoid allocation garbage */
	private AtomicBoolean allocationDirty = new AtomicBoolean();

	/** Empty list for requests without container asks */
	private final List<ResourceRequest> EMPTY = new ArrayList<ResourceRequest>();

	/** Groups for allocation tracking */
	private final AllocationGroups allocationGroups = new AllocationGroups();

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		internalInit();
	}

	@Override
	public void allocateContainers(int count) {
		if (log.isDebugEnabled()) {
			log.debug("Incoming count: " + count);
		}
		ContainerAllocateData cad = new ContainerAllocateData();
		cad.addAny(count);
		allocateContainers(cad);
	}

	@Override
	public void addListener(ContainerAllocatorListener listener) {
		allocatorListener.register(listener);
	}

	@Override
	public void allocateContainers(ContainerAllocateData containerAllocateData) {
		log.info("Incoming containerAllocateData: " + containerAllocateData);
		String id = StringUtils.hasText(containerAllocateData.getId()) ? containerAllocateData.getId() : "";
		AllocationGroup group = allocationGroups.get(id);

		if (group == null) {
			group = allocationGroups.get("");
		}

		boolean dirty = false;
		ContainerAllocateData cad = containerAllocateData.byAny();
		if (cad.hasData()) {
			dirty = true;
			DefaultAllocateCountTracker tracker = group.getAllocateCountTracker(AllocationGroup.GROUP_ANY);
			if (tracker == null) {
				allocationGroups.reserve(id, AllocationGroup.GROUP_ANY);
				tracker = new DefaultAllocateCountTracker(AllocationGroup.GROUP_ANY, getConfiguration());
				group.setAllocateCountTracker(AllocationGroup.GROUP_ANY, tracker);
			}
			log.info("State allocateCountTracker before adding allocation data: " + tracker);
			tracker.addContainers(cad);
			log.info("State allocateCountTracker after adding allocation data: " + tracker);
		}

		cad = containerAllocateData.byHosts();
		if (cad.hasData()) {
			dirty = true;
			DefaultAllocateCountTracker tracker = group.getAllocateCountTracker(AllocationGroup.GROUP_HOST);
			if (tracker == null) {
				allocationGroups.reserve(id, AllocationGroup.GROUP_HOST);
				tracker = new DefaultAllocateCountTracker(AllocationGroup.GROUP_HOST, getConfiguration());
				group.setAllocateCountTracker(AllocationGroup.GROUP_HOST, tracker);
			}
			log.info("State allocateCountTracker before adding allocation data: " + tracker);
			tracker.addContainers(cad);
			log.info("State allocateCountTracker after adding allocation data: " + tracker);
		}


		cad = containerAllocateData.byRacks();
		if (cad.hasData()) {
			dirty = true;
			DefaultAllocateCountTracker tracker = group.getAllocateCountTracker(AllocationGroup.GROUP_RACK);
			if (tracker == null) {
				allocationGroups.reserve(id, AllocationGroup.GROUP_RACK);
				tracker = new DefaultAllocateCountTracker(AllocationGroup.GROUP_RACK, getConfiguration());
				group.setAllocateCountTracker(AllocationGroup.GROUP_RACK, tracker);
			}
			log.info("State allocateCountTracker before adding allocation data: " + tracker);
			tracker.addContainers(cad);
			log.info("State allocateCountTracker after adding allocation data: " + tracker);
		}

		if (dirty) {
			allocationDirty.set(true);
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
		log.info("Adding new container to be released containerId=" + containerId);
		releaseContainers.add(containerId);
	}

	@Override
	protected AllocateResponse doContainerRequest() {
		List<ResourceRequest> requestedContainers = null;

		if (allocationDirty.getAndSet(false)) {
			requestedContainers = createRequests();
		} else {
			requestedContainers = EMPTY;
		}

		// add pending containers to be released
		List<ContainerId> release = new ArrayList<ContainerId>();
		ContainerId element = null;
		while ((element = releaseContainers.poll()) != null) {
			release.add(element);
		}

		if (log.isDebugEnabled()) {
			log.debug("Requesting containers using " + requestedContainers.size() + " requests.");
			for (ResourceRequest resourceRequest : requestedContainers) {
				log.debug("ResourceRequest: " + resourceRequest + " with count=" +
						resourceRequest.getNumContainers() + " with hostName=" + resourceRequest.getResourceName());
			}
			log.debug("Releasing containers " + release.size());
			for (ContainerId cid : release) {
				log.debug("Release container=" + cid);
			}
			log.debug("Request id will be: " + requestId.get());
		}

		// build the allocation request
		AllocateRequest request = Records.newRecord(AllocateRequest.class);
		request.setResponseId(requestId.get());
		request.setAskList(requestedContainers);
		request.setReleaseList(release);
		request.setProgress(applicationProgress);

		// do request and return response
		AllocateResponse allocate = getRmTemplate().allocate(request);
		requestId.set(allocate.getResponseId());
		return allocate;
	}

	@Override
	protected List<Container> preProcessAllocatedContainers(List<Container> containers) {
		// checking if we have standing allocation counts,
		// if not assume as garbage and send to release queue.
		// what's left will be out of our hand and expected to be
		// processed by the listener and eventually send back
		// to us as a released container.

		List<Container> preProcessed = new ArrayList<Container>();
		for (Container container : containers) {


			DefaultAllocateCountTracker tracker = allocationGroups.get(container.getPriority().getPriority())
					.getAllocateCountTracker(container.getPriority().getPriority());

			if (log.isDebugEnabled()) {
				log.debug("State allocateCountTracker before handling allocated container: " + tracker);
			}
			Container processed = tracker.processAllocatedContainer(container);
			if (processed != null) {
				preProcessed.add(processed);
			} else {
				garbageContainers.add(container.getId());
				releaseContainers.add(container.getId());
			}
			if (log.isDebugEnabled()) {
				log.debug("State allocateCountTracker after handling allocated container: " + tracker);
			}
		}
		return preProcessed;
	}

	@Override
	protected void handleAllocatedContainers(List<Container> containers) {
		allocatorListener.allocated(containers);
	}

	@Override
	protected void handleCompletedContainers(List<ContainerStatus> containerStatuses) {
		// strip away containers which were already marked
		// garbage by allocate tracker. system
		// never knew those even exist and might create mess
		// with monitor component. monitor only sees
		// complete status which is also the case for garbage
		// when it's released.
		List<ContainerStatus> garbageFree = new ArrayList<ContainerStatus>();
		for (ContainerStatus status : containerStatuses) {
			if (!garbageContainers.contains(status.getContainerId())) {
				garbageFree.add(status);
			}
		}
		allocatorListener.completed(garbageFree);
	}

	@Override
	public void setProgress(float progress) {
		applicationProgress = progress;
	}

	/**
	 * Sets the allocation values for given identifier.
	 *
	 * @param id the allocation identifier
	 * @param priority the base priority
	 * @param labelExpression the label expression
	 * @param virtualcores the cpu count
	 * @param memory the memory
	 * @param locality the locality flag
	 */
	public void setAllocationValues(String id, Integer priority, String labelExpression, Integer virtualcores, Long memory, Boolean locality) {
		if (log.isTraceEnabled()) {
			log.trace("setAllocationValues 1: id=" + id + " priority=" + priority + " labelExpression=" + labelExpression + " cores="
					+ virtualcores + " memory=" + memory + " locality=" + locality);
		}

		id = StringUtils.hasText(id) ? id : "";
		AllocationGroup group = allocationGroups.add(id, priority);
		group.setContainerAllocationValues(new ContainerAllocationValues(priority, labelExpression, virtualcores, memory, locality));
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
	 * Sets the label expression.
	 *
	 * @param labelExpression the new label expression
	 */
	public void setLabelExpression(String labelExpression) {
		this.labelExpression = labelExpression;
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
	public long getMemory() {
		return memory;
	}

	/**
	 * Sets the memory for container request defining
	 * <em>memory</em> of the resource.
	 *
	 * @param memory the new memory
	 */
	public void setMemory(long memory) {
		this.memory = memory;
	}

	/**
	 * Checks if is locality relax flag is enabled.
	 *
	 * @return true, if is locality is enabled.
	 */
	public boolean isLocality() {
		return locality;
	}

	/**
	 * Sets the flag telling if resource allocation
	 * should not be relaxed. Setting this to true
	 * means that relaxing is not used. Default value
	 * is false.
	 *
	 * @param locality the new locality relax flag
	 */
	public void setLocality(boolean locality) {
		this.locality = locality;
	}

	private List<ResourceRequest> createRequests() {
		List<ResourceRequest> requestedContainers = new ArrayList<ResourceRequest>();

		for (AllocationGroup group : allocationGroups.getGroups()) {
			for (DefaultAllocateCountTracker tracker : group.getAllocateCountTrackers()) {

				AllocateCountInfo allocateCounts = tracker.getAllocateCounts();
				Integer pri = group.getPriority(tracker.getId());
				ContainerAllocationValues allocationValues = group.getContainerAllocationValues();
				if (pri == null) {
					pri = allocationValues.priority;
				}

				if (log.isTraceEnabled()) {
					log.trace("trace 1 " + allocationValues.locality);
					log.trace("trace 2 tracker id:" + tracker.getId());
				}
				boolean hostsAdded = false;
				for (Entry<String, Integer> entry : allocateCounts.hostsInfo.entrySet()) {
					if (log.isTraceEnabled()) {
						log.trace("trace 3 entry key=" + entry.getKey() + " value=" + entry.getValue());
					}
					requestedContainers.add(getContainerResourceRequest(allocationValues, pri, entry.getValue(), entry.getKey(),
							true));
					hostsAdded = true;
				}
				for (Entry<String, Integer> entry : allocateCounts.racksInfo.entrySet()) {
					if (log.isTraceEnabled()) {
						log.trace("trace 4 entry key=" + entry.getKey() + " value=" + entry.getValue());
					}
					requestedContainers.add(getContainerResourceRequest(allocationValues, pri, entry.getValue(), entry.getKey(),
							(hostsAdded && allocationValues.locality) ? false : true));
				}
				for (Entry<String, Integer> entry : allocateCounts.anysInfo.entrySet()) {
					if (log.isTraceEnabled()) {
						log.trace("trace 5 entry key=" + entry.getKey() + " value=" + entry.getValue());
					}
					// need to force locality flag for any in case of
					// any group
					boolean localityForAny = tracker.getId().equals(AllocationGroup.GROUP_ANY) ? true : !allocationValues.locality;
					requestedContainers.add(getContainerResourceRequest(allocationValues, pri, entry.getValue(), entry.getKey(),
							localityForAny));
				}


			}
		}

		return requestedContainers;
	}

	/**
	 * Internal init which should only configure this class and
	 * should not touch parent classes.
	 */
	private void internalInit() {
		setAllocationValues(null, priority, labelExpression, virtualcores, memory, locality);
		for (DefaultAllocateCountTracker tracker : allocationGroups.getAllocateCountTrackers()) {
			tracker.setConfiguration(getConfiguration());
		}
	}

	/**
	 * Utility method creating a {@link ResourceRequest}.
	 *
	 * @param allocationValues
	 * @param priority
	 * @param numContainers
	 * @param hostName
	 * @param relaxLocality
	 * @return request to be sent to resource manager
	 */
	private ResourceRequest getContainerResourceRequest(ContainerAllocationValues allocationValues, int priority,
			int numContainers, String hostName, boolean relaxLocality) {
		ResourceRequest request = Records.newRecord(ResourceRequest.class);
		request.setRelaxLocality(relaxLocality);
		request.setResourceName(hostName);
		request.setNumContainers(numContainers);
		request.setNodeLabelExpression(allocationValues.labelExpression);
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(priority);
		request.setPriority(pri);
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemorySize(allocationValues.memory);
		ResourceCompat.setVirtualCores(capability, allocationValues.virtualcores);
		request.setCapability(capability);
		return request;
	}

}
