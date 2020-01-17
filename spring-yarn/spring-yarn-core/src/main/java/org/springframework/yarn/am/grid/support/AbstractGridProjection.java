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
	private final ConcurrentHashMap<String, Collection<GridMember>> hostCounts = new ConcurrentHashMap<String, Collection<GridMember>>();

	/** Tracking counts of mapped racks */
	private final ConcurrentHashMap<String, Collection<GridMember>> rackCounts = new ConcurrentHashMap<String, Collection<GridMember>>();

	/** Tracking count of anys */
	private final Collection<GridMember> anyCounts = new HashSet<GridMember>();

	/** Projection data which is a base for calculating a satisfy state for allocation */
	private ProjectionData projectionData;

	/** Hadoop configuration needed for hadoop's rack resolver */
	private Configuration configuration;

	/** Priority for this projection */
	private Integer priority;

	/** Resource memory setting */
	private Long memory;

	/** Resource cpu setting */
	private Integer virtualCores;

	/**
	 * Instantiates a new abstract grid projection.
	 */
	public AbstractGridProjection() {
	}

	/**
	 * Instantiates a new abstract grid projection.
	 *
	 * @param configuration the hadoop configuration
	 */
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
	public GridMember removeMember(GridMember member) {
		GridMember removed = members.remove(member.getContainer().getId());
		if (removed != null) {
			decrementHostCount(removed);
			decrementRackCount(removed);
			decrementAnyCount(removed);
		}
		return removed;
	}

	@Override
	public abstract boolean acceptMember(GridMember member);

	@Override
	public abstract SatisfyStateData getSatisfyState();

	/**
	 * Gets the priority.
	 *
	 * @return the priority
	 */
	@Override
	public Integer getPriority() {
		return priority;
	}

	/**
	 * Sets the priority.
	 *
	 * @param priority the new priority
	 */
	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	/**
	 * Sets the virtual cores.
	 *
	 * @param virtualCores the new virtual cores
	 */
	public void setVirtualCores(Integer virtualCores) {
		this.virtualCores = virtualCores;
	}

	/**
	 * Gets the virtual cores.
	 *
	 * @return the virtual cores
	 */
	public Integer getVirtualCores() {
		return virtualCores;
	}

	/**
	 * Sets the memory.
	 *
	 * @param memory the new memory
	 */
	public void setMemory(Long memory) {
		this.memory = memory;
	}

	/**
	 * Gets the memory.
	 *
	 * @return the memory
	 */
	public Long getMemory() {
		return memory;
	}

	/**
	 * Adds the member to a host tracking list.
	 *
	 * @param member the grid member
	 * @return true, if member were added
	 */
	protected boolean addHostMember(GridMember member) {
		Assert.notNull(member, "Member must not be null");
		if (members.putIfAbsent(member.getId(), member) == null) {
			incrementHostCount(member);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Adds the member to a rack tracking list.
	 *
	 * @param member the grid member
	 * @return true, if member were added
	 */
	protected boolean addRackMember(GridMember member) {
		Assert.notNull(member, "Member must not be null");
		if (members.putIfAbsent(member.getId(), member) == null) {
			incrementRackCount(member);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Adds the member to a any tracking list.
	 *
	 * @param member the grid member
	 * @return true, if member were added
	 */
	protected boolean addAnyMember(GridMember member) {
		Assert.notNull(member, "Member must not be null");
		if (members.putIfAbsent(member.getId(), member) == null) {
			incrementAnyCount(member);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Gets a count of hosts tracked by host.
	 *
	 * @param host the host
	 * @return the host count
	 */
	protected int getHostCount(String host) {
		Collection<GridMember> members = hostCounts.get(host);
		return members != null ? members.size() : 0;
	}

	/**
	 * Gets a count of racks tracked by rack.
	 *
	 * @param rack the rack
	 * @return the rack count
	 */
	protected int getRackCount(String rack) {
		Collection<GridMember> members = rackCounts.get(rack);
		return members != null ? members.size() : 0;
	}

	/**
	 * Gets the any count.
	 *
	 * @return the any count
	 */
	protected int getAnyCount() {
		return anyCounts.size();
	}

	/**
	 * Gets the host count members.
	 *
	 * @param host the host
	 * @return the host count members
	 */
	protected Collection<GridMember> getHostCountMembers(String host) {
		Collection<GridMember> members = hostCounts.get(host);
		return members != null ? members : Collections.<GridMember>emptyList();
	}

	/**
	 * Adds the host count member.
	 *
	 * @param host the host
	 * @param member the grid member
	 */
	protected void addHostCountMember(String host, GridMember member) {
		Collection<GridMember> m = hostCounts.get(host);
		if (m != null) {
			members.putIfAbsent(member.getId(), member);
			m.add(member);
		}
	}

	/**
	 * Adds the rack count member.
	 *
	 * @param rack the rack
	 * @param member the grid member
	 */
	protected void addRackCountMember(String rack, GridMember member) {
		Collection<GridMember> m = rackCounts.get(rack);
		if (m != null) {
			members.putIfAbsent(member.getId(), member);
			m.add(member);
		}
	}

	/**
	 * Gets the rack count members.
	 *
	 * @param rack the rack
	 * @return the rack count members
	 */
	protected Collection<GridMember> getRackCountMembers(String rack) {
		Collection<GridMember> members = rackCounts.get(rack);
		return members != null ? members : Collections.<GridMember>emptyList();
	}

	/**
	 * Gets the any count members.
	 *
	 * @return the any count members
	 */
	protected Collection<GridMember> getAnyCountMembers() {
		return anyCounts;
	}

	/**
	 * Gets the host count hosts.
	 *
	 * @return the host count hosts
	 */
	protected Set<String> getHostCountHosts() {
		return hostCounts.keySet();
	}

	/**
	 * Gets the rack count hosts.
	 *
	 * @return the rack count hosts
	 */
	protected Set<String> getRackCountHosts() {
		return rackCounts.keySet();
	}

	/**
	 * Sets the hadoop configuration.
	 *
	 * @param configuration the new hadoop configuration
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Gets the hadoop configuration.
	 *
	 * @return the hadoop configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Checks if a member can be fit in this projection.
	 *
	 * @param member the grid member
	 * @return true, if member can be fit
	 */
	protected boolean canFit(GridMember member) {
		int cpu = member.getContainer().getResource().getVirtualCores();
		long mem = member.getContainer().getResource().getMemorySize();
		return virtualCores != null && memory != null && virtualCores <= cpu && memory <= mem;
	}

	private boolean incrementHostCount(GridMember member) {
		String host = null;
		if (member.getContainer().getNodeId() != null) {
			host = member.getContainer().getNodeId().getHost();
		}
		if (host != null) {
			synchronized (hostCounts) {
				if (!hostCounts.containsKey(host)) {
					hostCounts.put(host, new HashSet<GridMember>());
				}
				return hostCounts.get(host).add(member);
			}
		}
		return false;
	}

	private boolean incrementRackCount(GridMember member) {
		String rack = resolveRack(member);
		if (rack != null) {
			synchronized (rackCounts) {
				if (!rackCounts.containsKey(rack)) {
					rackCounts.put(rack, new HashSet<GridMember>());
				}
				return rackCounts.get(rack).add(member);
			}
		}
		return false;
	}

	private boolean incrementAnyCount(GridMember member) {
		String host = null;
		if (member.getContainer().getNodeId() != null) {
			host = member.getContainer().getNodeId().getHost();
		}
		if (host != null) {
			return anyCounts.add(member);
		}
		return false;
	}

	private boolean decrementRackCount(GridMember member) {
		String rack = resolveRack(member);
		if (rack != null && rackCounts.get(rack) != null) {
			return rackCounts.get(rack).remove(member);
		}
		return false;
	}

	private boolean decrementHostCount(GridMember member) {
		String host = null;
		if (member.getContainer().getNodeId() != null) {
			host = member.getContainer().getNodeId().getHost();
		}
		if (host != null && hostCounts.get(host) != null) {
			return hostCounts.get(host).remove(member);
		}
		return false;
	}

	private boolean decrementAnyCount(GridMember member) {
		String host = null;
		if (member.getContainer().getNodeId() != null) {
			host = member.getContainer().getNodeId().getHost();
		}
		if (host != null) {
			return anyCounts.remove(member);
		}
		return false;
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

}
