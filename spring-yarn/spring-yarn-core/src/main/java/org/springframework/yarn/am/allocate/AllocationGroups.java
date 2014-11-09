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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Allocation groups is keeping a logic for allocation values, allocation trackers
 * and reserved priorities in one place.
 *
 * @author Janne Valkealahti
 *
 */
class AllocationGroups {

	/**
	 * Mapping of existing allocation groups to its id's
	 */
	private final Map<String, AllocationGroup> groups = new ConcurrentHashMap<String, AllocationGroup>();

	/** Set of reserved priorities. */
	private final Set<Integer> reservedPriorities = new HashSet<Integer>();

	/**
	 * Gets an allocation group by its identifier.
	 *
	 * @param id the group identifier
	 * @return the allocation group or null if group doesn't exist
	 */
	public AllocationGroup get(String id) {
		return groups.get(id);
	}

	/**
	 * Gets all registered groups.
	 *
	 * @return all registered groups
	 */
	public Collection<AllocationGroup> getGroups() {
		return groups.values();
	}

	/**
	 * Gets an allocation group which contains reservation for a priority.
	 *
	 * @param priority a priority for group
	 * @return the allocation group or null if group doesn't exist
	 */
	public AllocationGroup get(Integer priority) {
		if (priority == null) {
			return null;
		}
		for (AllocationGroup group : groups.values()) {
			if (group.belongs(priority)) {
				return group;
			}
		}
		return null;
	}

	/**
	 * Add new allocation group using identifier and group's
	 * base priority.
	 *
	 * @param id group identifier
	 * @param basePriority group base priority
	 * @return the created group
	 */
	public AllocationGroup add(String id, Integer basePriority) {
		AllocationGroup group = get(id);
		if (group != null) {
			throw new IllegalArgumentException("Group with id=" + id + " allready added");
		}
		group = new AllocationGroup(basePriority, id);
		groups.put(id, group);
		return group;
	}

	/**
	 * Does a reservation with a group matched with identifier
	 * using a priority identifier.
	 *
	 * @param id group identifier
	 * @param priorityId the priority identifier
	 * @return the existing group
	 */
	public AllocationGroup reserve(String id, String priorityId) {
		AllocationGroup group = get(id);
		if (group != null) {
			Integer nextFreePriority = reserveNextFreePriority(group.getBasePriority());
			group.addPriority(priorityId, nextFreePriority);
		}
		// TODO should we throw if id doesn't match to existing group?
		return group;
	}

	/**
	 * Gets all {@link DefaultAllocateCountTracker}s known to {@link AllocationGroup}s.
	 *
	 * @return all {@link DefaultAllocateCountTracker}s
	 */
	public Collection<DefaultAllocateCountTracker> getAllocateCountTrackers() {
		ArrayList<DefaultAllocateCountTracker> trackers = new ArrayList<DefaultAllocateCountTracker>();
		for (AllocationGroup group : groups.values()) {
			trackers.addAll(group.getAllocateCountTrackers());
		}
		return trackers;
	}

	/**
	 * Makes a reservation of next available free priority.
	 *
	 * @param startPriority a point where to start finding free priorities
	 * @return the reserved priority
	 */
	private Integer reserveNextFreePriority(int startPriority) {
		while (!reservedPriorities.add(startPriority)) {
			startPriority++;
		}
		return startPriority;
	}

}
