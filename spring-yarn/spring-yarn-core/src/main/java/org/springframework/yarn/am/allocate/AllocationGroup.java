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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Allocation group is keeping allocation values and allocation trackers together.
 *
 * @author Janne Valkealahti
 *
 */
class AllocationGroup {

	public static final String GROUP_ANY = "any";

	public static final String GROUP_HOST = "host";

	public static final String GROUP_RACK = "rack";

	private final Map<String, Integer> idToPriMap = new HashMap<String, Integer>();

	private final Map<Integer, String> priToIdMap = new HashMap<Integer, String>();

	private final Map<String, DefaultAllocateCountTracker> trackers = new HashMap<String, DefaultAllocateCountTracker>();

	private final Integer basePriority;

	private final String id;

	private ContainerAllocationValues containerAllocationValues;

	/**
	 * Instantiates a new allocation group.
	 *
	 * @param basePriority the base priority
	 * @param id the group identifier
	 */
	public AllocationGroup(Integer basePriority, String id) {
		this.basePriority = basePriority;
		this.id = id;
	}

	/**
	 * Gets the base priority.
	 *
	 * @return the base priority
	 */
	public Integer getBasePriority() {
		return basePriority;
	}

	/**
	 * Adds the priority mapped with identifier.
	 *
	 * @param priorityId the priority id
	 * @param priority the priority
	 */
	public void addPriority(String priorityId, Integer priority) {
		idToPriMap.put(priorityId, priority);
		priToIdMap.put(priority, priorityId);
	}

	/**
	 * Gets the priority by its mapped id.
	 *
	 * @param priorityId the priority id
	 * @return the priority
	 */
	public Integer getPriority(String priorityId) {
		return idToPriMap.get(priorityId);
	}

	/**
	 * Belongs.
	 *
	 * @param priority the priority
	 * @return true, if successful
	 */
	public boolean belongs(int priority) {
		return basePriority.intValue() == priority || idToPriMap.values().contains(priority);
	}

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * Sets the container allocation values.
	 *
	 * @param containerAllocationValues the new container allocation values
	 */
	public void setContainerAllocationValues(ContainerAllocationValues containerAllocationValues) {
		this.containerAllocationValues = containerAllocationValues;
	}

	/**
	 * Gets the container allocation values.
	 *
	 * @return the container allocation values
	 */
	public ContainerAllocationValues getContainerAllocationValues() {
		return containerAllocationValues;
	}

	/**
	 * Sets the allocate count tracker.
	 *
	 * @param subId the sub id
	 * @param defaultAllocateCountTracker the default allocate count tracker
	 */
	public void setAllocateCountTracker(String subId, DefaultAllocateCountTracker defaultAllocateCountTracker) {
		trackers.put(subId, defaultAllocateCountTracker);
	}

	/**
	 * Gets the allocate count tracker.
	 *
	 * @param subId the sub id
	 * @return the allocate count tracker
	 */
	public DefaultAllocateCountTracker getAllocateCountTracker(String subId) {
		return trackers.get(subId);
	}

	/**
	 * Gets the allocate count trackers.
	 *
	 * @return the allocate count trackers
	 */
	public Collection<DefaultAllocateCountTracker> getAllocateCountTrackers() {
		return trackers.values();
	}

	/**
	 * Gets the allocate count tracker.
	 *
	 * @param priority the priority
	 * @return the allocate count tracker
	 */
	public DefaultAllocateCountTracker getAllocateCountTracker(Integer priority) {
		return trackers.get(priToIdMap.get(priority));
	}

	public static class ContainerAllocationValues {
		int priority = 0;
		String labelExpression;
		int virtualcores = 1;
		long memory = 64;
		boolean locality = false;
		public ContainerAllocationValues(Integer priority, String labelExpression, Integer virtualcores, Long memory, Boolean locality) {
			if (priority != null) {
				this.priority = priority;
			}
			this.labelExpression = labelExpression;
			if (virtualcores != null) {
				this.virtualcores = virtualcores;
			}
			if (memory != null) {
				this.memory = memory;
			}
			if (locality != null) {
				this.locality = locality;
			}
		}

		@Override
		public String toString() {
			return "ContainerAllocationValues [priority=" + priority + ", labelExpression=" + labelExpression + ", virtualcores="
					+ virtualcores + ", memory=" + memory + ", locality=" + locality + "]";
		}

	}

}
