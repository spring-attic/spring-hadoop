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

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Acts as a main entry point abstracting access for
 * {@link org.apache.hadoop.yarn.api.records.ResourceRequest ResourceRequest}s.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerAllocateData {

	private String id;
	private Integer anyData = 0;
	private Map<String, Integer> hostData = new ConcurrentHashMap<String, Integer>();
	private Map<String, Integer> rackData = new ConcurrentHashMap<String, Integer>();

	/**
	 * Instantiates a new container allocate data.
	 */
	public ContainerAllocateData() {
	}

	/**
	 * Adds request for a rack for given count.
	 *
	 * @param name the rack name
	 * @param count the container count
	 */
	public void addRacks(String name, int count) {
		synchronized (rackData) {
			Integer value = rackData.get(name);
			if (value == null) {
				rackData.put(name, count);
			} else {
				rackData.put(name, value + count);
			}
		}
	}

	/**
	 * Gets the racks counts.
	 *
	 * @return the racks count map
	 */
	public Map<String, Integer> getRacks() {
		return rackData;
	}

	/**
	 * Adds request for a host for given count.
	 *
	 * @param name the host name
	 * @param count the container count
	 */
	public void addHosts(String name, int count) {
		synchronized (hostData) {
			Integer value = hostData.get(name);
			if (value == null) {
				hostData.put(name, count);
			} else {
				hostData.put(name, value + count);
			}
		}
	}

	/**
	 * Gets the hosts counts.
	 *
	 * @return the hosts count map
	 */
	public Map<String, Integer> getHosts() {
		return hostData;
	}

	/**
	 * Adds request for any container for given count.
	 *
	 * @param count the count
	 */
	public void addAny(int count) {
		synchronized (anyData) {
			anyData = anyData + count;
		}
	}

	/**
	 * Gets the count for any container.
	 *
	 * @return the any
	 */
	public int getAny() {
		return anyData;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public boolean hasData() {
		return anyData > 0 || !hostData.isEmpty() || !rackData.isEmpty();
	}

	public ContainerAllocateData byAny() {
		ContainerAllocateData cad = new ContainerAllocateData();
		cad.anyData = getAny();
		return cad;
	}

	public ContainerAllocateData byHosts() {
		ContainerAllocateData cad = new ContainerAllocateData();
		cad.hostData = getHosts();
		return cad;
	}

	public ContainerAllocateData byRacks() {
		ContainerAllocateData cad = new ContainerAllocateData();
		cad.rackData = getRacks();
		return cad;
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();

		buf.append("[anyData=" + anyData + ", ");

		buf.append("hostData=[size=" + hostData.size() + ", ");
		buf.append("{");
		for (Entry<String, Integer> entry : hostData.entrySet()) {
			buf.append(entry.getKey() + "=" + entry.getValue() + " ");
		}
		buf.append("}], ");

		buf.append("rackData=[size=" + rackData.size() + ", ");
		buf.append("{");
		for (Entry<String, Integer> entry : rackData.entrySet()) {
			buf.append(entry.getKey() + "=" + entry.getValue() + " ");
		}
		buf.append("}]]");

		return buf.toString();
	}

}
