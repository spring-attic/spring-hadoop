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

public class ProjectionData {

	private String type;
	private Integer priority;
	private Integer any = 0;
	private Map<String, Integer> hosts = new HashMap<String, Integer>();
	private Map<String, Integer> racks = new HashMap<String, Integer>();

	public ProjectionData() {
	}

	public ProjectionData(Integer any) {
		this(any, null, null);
	}

	public ProjectionData(Integer any, Map<String, Integer> hosts, Map<String, Integer> racks) {
		this(any, hosts, racks, null, null);
	}

	public ProjectionData(Integer any, Map<String, Integer> hosts, Map<String, Integer> racks, String type, Integer priority) {
		this.any = any;
		this.hosts = hosts;
		this.racks = racks;
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
		this.priority = priority;
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
		this.hosts = hosts;
	}

	public Map<String, Integer> getRacks() {
		return racks;
	}

	public void setRacks(Map<String, Integer> racks) {
		this.racks = racks;
	}

	public void setHost(String host, Integer count) {
		hosts.put(host, count);
	}

	public void setRack(String rack, Integer count) {
		racks.put(rack, count);
	}

	@Override
	public String toString() {
		return "ProjectionData [type=" + type + ", priority=" + priority + ", any=" + any + ", hosts=" + hosts
				+ ", racks=" + racks + "]";
	}

}
