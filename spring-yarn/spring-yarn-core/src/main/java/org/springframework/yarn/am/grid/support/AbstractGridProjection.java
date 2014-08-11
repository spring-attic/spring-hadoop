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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.util.RackResolver;
import org.springframework.util.Assert;
import org.springframework.yarn.am.grid.GridMember;
import org.springframework.yarn.am.grid.GridProjection;

/**
 * Base implementation of a {@link GridProjection}.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractGridProjection implements GridProjection {

	private static final Log log = LogFactory.getLog(AbstractGridProjection.class);

	/** Member mapping from container id to grid member */
	private final ConcurrentHashMap<ContainerId, GridMember> members = new ConcurrentHashMap<ContainerId, GridMember>();

	/** Tracking counts of mapped hosts */
	private final ConcurrentHashMap<String, HostCountHolder> hostCounts = new ConcurrentHashMap<String, HostCountHolder>();

	/** Tracking counts of mapped racks */
	private final ConcurrentHashMap<String, HostCountHolder> rackCounts = new ConcurrentHashMap<String, HostCountHolder>();

	/** Projection data which is a base for calculating a satisfy state for allocation */
	private ProjectionData projectionData;

	/** Hadoop configuration needed for hadoop's rack resolver */
	private Configuration configuration;

	private Integer priority;

	/**
	 * Instantiates a new abstract grid projection.
	 */
	public AbstractGridProjection() {
	}

	public AbstractGridProjection(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public Collection<GridMember> getMembers() {
		return members.values();
	}

	@Override
	public void setProjectionData(ProjectionData data) {
		projectionData = data;
	}

	@Override
	public ProjectionData getProjectionData() {
		return projectionData;
	}

	@Override
	public abstract boolean acceptMember(GridMember member);

	@Override
	public abstract SatisfyStateData getSatisfyState();

	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	public Integer getPriority() {
		return priority;
	}

	protected boolean addMember(GridMember member) {
		Assert.notNull(member, "Node must not be null");
		if (members.putIfAbsent(member.getId(), member) == null) {
			incrementHostCount(member);
			incrementRackCount(member);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public GridMember removeMember(GridMember member) {
		GridMember removed = members.remove(member.getContainer().getId());
		if (removed != null) {
			decrementHostCount(removed);
			decrementRackCount(removed);
		}
		return removed;
	}

	protected int getHostCount(String host) {
		HostCountHolder holder = hostCounts.get(host);
		return holder != null ? holder.count : 0;
	}

	protected int getRackCount(String rack) {
		HostCountHolder holder = rackCounts.get(rack);
		return holder != null ? holder.count : 0;
	}

	protected Collection<GridMember> getHostCountMembers(String host) {
		HostCountHolder holder = hostCounts.get(host);
		return holder != null ? holder.members : Collections.<GridMember>emptyList();
	}

	protected Collection<GridMember> getRackCountMembers(String host) {
		HostCountHolder holder = rackCounts.get(host);
		return holder != null ? holder.members : Collections.<GridMember>emptyList();
	}

	protected Set<String> getHostCountHosts() {
		return hostCounts.keySet();
	}

	protected Set<String> getRackCountHosts() {
		return rackCounts.keySet();
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	protected boolean isSamePriority(GridMember member) {
		Priority pri = member.getContainer().getPriority();
		if (pri != null && getPriority() != null) {
			return pri.getPriority() == getPriority().intValue();
		}
		return false;
	}

	private void incrementHostCount(GridMember member) {
		String host = null;
		if (member.getContainer().getNodeId() != null) {
			host = member.getContainer().getNodeId().getHost();
		}
		if (host != null) {
			if (!hostCounts.containsKey(host)) {
				hostCounts.put(host, new HostCountHolder());
			}
			hostCounts.get(host).add(member);
		}
	}

	private void incrementRackCount(GridMember member) {
		String rack = resolveRack(member);
		if (rack != null) {
			if (!rackCounts.containsKey(rack)) {
				rackCounts.put(rack, new HostCountHolder());
			}
			rackCounts.get(rack).add(member);
		}
	}

	private void decrementRackCount(GridMember member) {
		String rack = resolveRack(member);
		if (rack != null) {
			rackCounts.get(rack).remove(member);
		}
	}

	private void decrementHostCount(GridMember member) {
		String host = null;
		if (member.getContainer().getNodeId() != null) {
			host = member.getContainer().getNodeId().getHost();
		}
		if (host != null) {
			hostCounts.get(host).remove(member);
		}
	}

	private String resolveRack(GridMember member) {
		if (getConfiguration() != null) {
			String host = member.getContainer().getNodeId().getHost();
			String rack = RackResolver.resolve(getConfiguration(), host).getNetworkLocation();
			if (rack == null) {
				log.warn("Failed to resolve rack for node " + host + ".");
			} else {
				log.info("Resolve rack for node " + host + " into " + rack);
			}
			return rack;
		} else {
			return null;
		}
	}

	private static class HostCountHolder {
		Integer count = 0;
		Collection<GridMember> members = new HashSet<GridMember>();
		void add(GridMember member) {
			if (members.add(member)) {
				count++;
			}
		}
		void remove(GridMember member) {
			if (members.remove(member)) {
				count--;
			}
		}
	}

}
