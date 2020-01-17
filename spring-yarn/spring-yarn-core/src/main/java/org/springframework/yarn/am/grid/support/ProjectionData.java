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
package org.springframework.yarn.am.grid.support;

import java.util.HashMap;
import java.util.Map;

/**
 * Data object which works between a configuration and projections.
 *
 * @author Janne Valkealahti
 *
 */
public class ProjectionData {

	private String type;
	private Integer priority;
	private Long memory;
	private Integer virtualCores;
	private Boolean locality;
	private Integer any = 0;
	private Map<String, Integer> hosts = new HashMap<String, Integer>();
	private Map<String, Integer> racks = new HashMap<String, Integer>();
	private Map<String, Object> properties = new HashMap<String, Object>();

	/**
	 * Instantiates a new projection data.
	 */
	public ProjectionData() {
	}

	/**
	 * Instantiates a new projection data.
	 *
	 * @param any the any count
	 */
	public ProjectionData(Integer any) {
		this(any, null, null);
	}

	/**
	 * Instantiates a new projection data.
	 *
	 * @param any the any count
	 * @param hosts the host count map
	 * @param racks the rack count map
	 */
	public ProjectionData(Integer any, Map<String, Integer> hosts, Map<String, Integer> racks) {
		this(any, hosts, racks, null, null);
	}

	/**
	 * Instantiates a new projection data.
	 *
	 * @param any the any count
	 * @param hosts the host count map
	 * @param racks the rack count map
	 * @param properties the extra properties
	 */
	public ProjectionData(Integer any, Map<String, Integer> hosts, Map<String, Integer> racks,
			Map<String, Object> properties) {
		this(any, hosts, racks, properties, null, null);
	}

	/**
	 * Instantiates a new projection data.
	 *
	 * @param any the any count
	 * @param hosts the host count map
	 * @param racks the rack count map
	 * @param type the type
	 * @param priority the priority
	 */
	public ProjectionData(Integer any, Map<String, Integer> hosts, Map<String, Integer> racks, String type,
			Integer priority) {
		this(any, hosts, racks, null, type, priority);
	}

	/**
	 * Instantiates a new projection data.
	 *
	 * @param any the any count
	 * @param hosts the host count map
	 * @param racks the rack count map
	 * @param properties the extra properties
	 * @param type the type
	 * @param priority the priority
	 */
	public ProjectionData(Integer any, Map<String, Integer> hosts, Map<String, Integer> racks,
			Map<String, Object> properties, String type, Integer priority) {
		this.any = any;
		if (hosts != null) {
			this.hosts = hosts;
		}
		if (racks != null) {
			this.racks = racks;
		}
		if (properties != null) {
			this.properties = properties;
		}
		this.type = type;
		this.priority = priority;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Integer getPriority() {
		return priority;
	}

	public void setPriority(Integer priority) {
		if (priority != null) {
			this.priority = priority;
		}
	}

	public void setVirtualCores(Integer virtualCores) {
		this.virtualCores = virtualCores;
	}

	public Integer getVirtualCores() {
		return virtualCores;
	}

	public void setMemory(Long memory) {
		this.memory = memory;
	}

	public void setLocality(Boolean locality) {
		this.locality = locality;
	}

	public Boolean getLocality() {
		return locality;
	}

	public Long getMemory() {
		return memory;
	}

	public Integer getAny() {
		return any;
	}

	public void setAny(int any) {
		this.any = any;
	}

	public Map<String, Integer> getHosts() {
		return hosts;
	}

	public void setHosts(Map<String, Integer> hosts) {
		if (hosts != null) {
			this.hosts = hosts;
		}
	}

	public Map<String, Integer> getRacks() {
		return racks;
	}

	public void setRacks(Map<String, Integer> racks) {
		if (racks != null) {
			this.racks = racks;
		}
	}

	public void setHost(String host, Integer count) {
		hosts.put(host, count);
	}

	public void setRack(String rack, Integer count) {
		racks.put(rack, count);
	}

	public Map<String, Object> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;
	}

	/**
	 * Merge projection data to this instance by setting fields
	 * from other if set. This method returns a new
	 * instance of {@link ProjectionData}.
	 *
	 * @param other the projection to merge
	 * @return the new merged projection data
	 */
	public ProjectionData merge(ProjectionData other) {
		ProjectionData data = new ProjectionData();
		data.type = other.type != null ? other.type : this.type;
		data.priority = other.priority != null ? other.priority : this.priority;
		data.memory = other.memory != null ? other.memory : this.memory;
		data.virtualCores = other.virtualCores != null ? other.virtualCores : this.virtualCores;
		data.locality = other.locality != null ? other.locality : this.locality;
		data.any = (other.any != null && other.any > 0) ? other.any : this.any;
		if (other.hosts != null) {
			data.hosts.putAll(other.hosts);
		}
		if (other.racks != null) {
			data.racks.putAll(other.racks);
		}
		if (other.properties != null) {
			data.properties.putAll(other.properties);
		}
		return data;
	}

	@Override
	public String toString() {
		return "ProjectionData [type=" + type + ", priority=" + priority + ", memory=" + memory + ", virtualCores="
				+ virtualCores + ", locality=" + locality + ", any=" + any + ", hosts=" + hosts + ", racks=" + racks
				+ ", properties=" + properties + "]";
	}

}
