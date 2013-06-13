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
package org.springframework.yarn.am.container;

import java.util.Arrays;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * Helper class used to add request data hints. None of
 * the parameters set here are not guaranteed to be
 * used. For example setting an array of hosts and racks
 * just tells user of this class that we'd like to
 * localise into those locations.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerRequestHint {

	/** Id acts as a convenience field for user to be used for key */
	private final Object id;

	private final Resource capability;
	private final String[] hosts;
	private final String[] racks;
	private final Priority priority;

	/**
	 * Instantiates a new container request data.
	 *
	 * @param id the id
	 * @param capability the capability
	 * @param hosts the hosts
	 * @param racks the racks
	 * @param priority the priority
	 */
	public ContainerRequestHint(Object id, Resource capability, String[] hosts, String[] racks, Priority priority) {
		this.id = id;
		this.capability = capability;
		this.hosts = hosts;
		this.racks = racks;
		this.priority = priority;
	}

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public Object getId() {
		return id;
	}

	/**
	 * Gets the capability.
	 *
	 * @return the capability
	 */
	public Resource getCapability() {
		return capability;
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
	 * Gets the racks.
	 *
	 * @return the racks
	 */
	public String[] getRacks() {
		return racks;
	}

	/**
	 * Gets the priority.
	 *
	 * @return the priority
	 */
	public Priority getPriority() {
		return priority;
	}

	@Override
	public String toString() {
		return "ContainerRequestData [id=" + id + ", capability=" + capability + ", hosts=" + Arrays.toString(hosts)
				+ ", racks=" + Arrays.toString(racks) + ", priority=" + priority + "]";
	}

}
