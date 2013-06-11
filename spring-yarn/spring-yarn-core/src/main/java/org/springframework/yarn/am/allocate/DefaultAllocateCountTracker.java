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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;

/**
 * Helper class tracking allocation counts. This separates counts
 * for hosts into two states; in first state we have a counts of pending
 * requests which are not yet sent into resource manager. in second state
 * we have a counts which are sent into resource manager. These states
 * allows us to loosely track needed counts sent during the allocation
 * requests to minimise allocation garbage.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultAllocateCountTracker {

	private static final Log log = LogFactory.getLog(DefaultAllocateCountTracker.class);

	/** Incoming request counts for hosts */
	private Map<String, AtomicInteger> pendingHosts = new HashMap<String, AtomicInteger>();

	/** Counts for hosts requested and not yet received */
	private Map<String, AtomicInteger> requestedHosts = new HashMap<String, AtomicInteger>();

	/** Incoming request counts for racks */
	private Map<String, AtomicInteger> pendingRacks = new HashMap<String, AtomicInteger>();

	/** Counts for racks requested and not yet received */
	private Map<String, AtomicInteger> requestedRacks = new HashMap<String, AtomicInteger>();

	/** Incoming request counts for any */
	private AtomicInteger pendingAny = new AtomicInteger();

	/** Counts for anys requested and not yet received */
	private AtomicInteger requestedAny = new AtomicInteger();

	/**
	 * Adds new count of containers into 'any'
	 * pending requests.
	 *
	 * @param count the count to add
	 */
	public void addContainers(int count) {
		if (count > 0) {
			int ncount = pendingAny.addAndGet(count);
			if(log.isDebugEnabled()) {
				log.debug("Adding " + count + " to pendingAny. New count is " + ncount);
			}
		}
	}

	/**
	 * Adds new count of containers into 'host', 'rack'
	 * and 'any' pending requests.
	 *
	 * @param containerAllocateData the container allocate data
	 */
	public void addContainers(ContainerAllocateData containerAllocateData) {
		// Adding incoming host counts to internal map
		Iterator<Entry<String, Integer>> iterator = containerAllocateData.getHosts().entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, Integer> entry = iterator.next();
			AtomicInteger atomicInteger = pendingHosts.get(entry.getKey());
			if (atomicInteger == null) {
				atomicInteger = new AtomicInteger();
				pendingHosts.put(entry.getKey(), atomicInteger);
			}
			atomicInteger.addAndGet(entry.getValue());
		}

		// Adding incoming rack counts to internal map
		iterator = containerAllocateData.getRacks().entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, Integer> entry = iterator.next();
			AtomicInteger atomicInteger = pendingRacks.get(entry.getKey());
			if (atomicInteger == null) {
				atomicInteger = new AtomicInteger();
				pendingRacks.put(entry.getKey(), atomicInteger);
			}
			atomicInteger.addAndGet(entry.getValue());
		}

		// Adding incoming any count
		addContainers(containerAllocateData.getAny());
	}

	/**
	 * Gets the allocate counts which should be used
	 * to create allocate requests.
	 *
	 * @return the allocate counts
	 */
	public Map<String, Integer> getAllocateCounts() {
		HashMap<String, Integer> allocateCountMap = new HashMap<String, Integer>();

		int total = 0;

		// flush pending hosts from incoming to outgoing
		Iterator<Entry<String, AtomicInteger>> iterator = pendingHosts.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, AtomicInteger> entry = iterator.next();
			int value = entry.getValue().getAndSet(0);
			allocateCountMap.put(entry.getKey(), value);
			AtomicInteger out = requestedHosts.get(entry.getKey());
			if (out == null) {
				out = new AtomicInteger(value);
				requestedHosts.put(entry.getKey(), out);
			} else {
				out.getAndAdd(value);
			}
			total += out.get();
		}

		// flush pending racks from incoming to outgoing
		iterator = pendingRacks.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, AtomicInteger> entry = iterator.next();
			int value = entry.getValue().getAndSet(0);
			allocateCountMap.put(entry.getKey(), value);
			AtomicInteger out = requestedRacks.get(entry.getKey());
			if (out == null) {
				out = new AtomicInteger(value);
				requestedRacks.put(entry.getKey(), out);
			} else {
				out.getAndAdd(value);
			}
			total += out.get();
		}

		// this is a point where allocation request gets tricky. Allocation will not happen
		// until "*" is sent as a hostname. Also count for "*" has to match total of hosts and
		// racks to be requested. Also count need to include any general "*" requests.
		// we'll try to calculate as accurate number as we can not to get too much
		// garbage if user is ramping up request throughout the AM lifecycle.
		int value = requestedAny.addAndGet(pendingAny.getAndSet(0));
		total += value;
		allocateCountMap.put("*", total);

		return allocateCountMap;
	}

	public Container processAllocatedContainer(Container container) {
		String host = container.getNodeId().getHost();

		if (modifyWithKey(requestedHosts, host, false)) {
			// match hosts
			log.debug("Found reservation match from hosts for " + host);
		} else if (modifyWithKey(requestedRacks, host, false)) {
			// match racks
			log.debug("Found reservation match from racks for " + host);
		} else if (modify(requestedAny, false)) {
			// match anys
			log.debug("Found reservation match from anys for " + host);
		} else if (decrement(requestedHosts)) {
			// no match - just start to flush out outstanding requests
			log.debug("No reservation match for " + host + ", decremented hosts");
		} else if (decrement(requestedRacks)) {
			// no match - just start to flush out outstanding requests
			log.debug("No reservation match for " + host + ", decremented racks");
		} else if (decrement(requestedAny)) {
			// no match - just start to flush out outstanding requests
			log.debug("No reservation match for " + host + ", decremented anys");
		} else {
			// no outstanding requests - mark as garbage
			log.debug("No outstanding requests, marking as garbage");
			return null;
		}

		return container;
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append('[');
		buf.append("pendingHosts size=" + pendingHosts.size() + " map=" + mapToDebugString(pendingHosts) + ", ");
		buf.append("requestedHosts size=" + requestedHosts.size() + " map=" + mapToDebugString(requestedHosts) + ", ");
		buf.append("pendingRacks size=" + pendingRacks.size() + " map=" + mapToDebugString(pendingRacks) + ", ");
		buf.append("requestedRacks size=" + requestedRacks.size() + " map=" + mapToDebugString(requestedRacks) + ", ");
		buf.append("pendingAny size=" + pendingAny.get() + ", ");
		buf.append("requestedAny size=" + requestedAny.get());
		buf.append(']');
		return buf.toString();
	}

	/**
	 * Decrement a value. Value is kept as non-negative.
	 *
	 * @param value the value to decrement
	 * @return true, if any value is modified, false otherwise
	 */
	private static boolean decrement(AtomicInteger value) {
		if (value.get() > 0) {
			value.decrementAndGet();
			return true;
		}
		return false;
	}

	/**
	 * Decrement a first positive entry value from a map.
	 * Values are kept as non-negatives.
	 *
	 * @param map the map to search entry values
	 * @return true, if any value is modified, false otherwise
	 */
	private static boolean decrement(Map<String, AtomicInteger> map) {
		boolean match = false;
		Iterator<Entry<String, AtomicInteger>> iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, AtomicInteger> entry = iterator.next();
			if (entry.getValue().get() > 0) {
				entry.getValue().decrementAndGet();
				match = true;
				break;
			}
		}
		return match;
	}

	/**
	 * Modify {@link AtomicInteger} matched by key from a map
	 * either by incrementing or decrementing value. Value is
	 * always kept as non-negative.
	 *
	 * @param map the map to search for value
	 * @param key the key to find the value
	 * @param increment if true increment, if false decrement
	 * @return true, if value is modified, false otherwise
	 */
	private static boolean modifyWithKey(Map<String, AtomicInteger> map, String key, boolean increment) {
		AtomicInteger value = map.get(key);
		if (value != null) {
			if (increment) {
				value.incrementAndGet();
			} else {
				if (value.get() > 0) {
					value.decrementAndGet();
					return true;
				} else {
					return false;
				}
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Modify {@link AtomicInteger} either by incrementing
	 * or decrementing value. Value is always kept as
	 * non-negative.
	 *
	 * @param value the value to modify
	 * @param increment if true increment, if false decrement
	 * @return true, if value is modified, false otherwise
	 */
	private static boolean modify(AtomicInteger value, boolean increment) {
		if (increment) {
			value.incrementAndGet();
			return true;
		} else {
			if (value.get() > 0) {
				value.decrementAndGet();
				return true;
			} else {
				return false;
			}
		}
	}

	/**
	 * Creates a debug representation of map.
	 *
	 * @param map the map
	 * @return the state of map
	 */
	private static String mapToDebugString(Map<String, AtomicInteger> map) {
		StringBuilder buf = new StringBuilder();
		buf.append('{');
		Iterator<Entry<String, AtomicInteger>> iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, AtomicInteger> entry = iterator.next();
			buf.append(entry.getKey() + "=" + entry.getValue());
			if (iterator.hasNext()) {
				buf.append(", ");
			}
		}
		buf.append('}');
		return buf.toString();
	}

}
