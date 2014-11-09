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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.util.RackResolver;
import org.springframework.yarn.am.grid.GridMember;
import org.springframework.yarn.am.grid.GridProjection;

/**
 * Default implementation of {@link GridProjection}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultGridProjection extends AbstractGridProjection {

	private static final Log log = LogFactory.getLog(DefaultGridProjection.class);

	public static final String REGISTERED_NAME = "default";

	private boolean locality = true;

	/**
	 * Instantiates a new default grid projection.
	 */
	public DefaultGridProjection() {
		super();
	}

	@Override
	public SatisfyStateData getSatisfyState() {
		SatisfyStateData data = new SatisfyStateData();
		ArrayList<GridMember> remove = new ArrayList<GridMember>();

		// any

		// simply add delta for current size vs. requested count
		int delta = (getProjectionData() != null ? getProjectionData().getAny() : 0) - getAnyCountMembers().size();
		data.getAllocateData().addAny(Math.max(delta, 0));

		// Simply remove using negative delta
		int removeCount = Math.max(-delta, 0);
		Iterator<GridMember> iterator = getAnyCountMembers().iterator();
		while (iterator.hasNext() && removeCount-- > 0) {
			GridMember next = iterator.next();
			log.debug("Adding " + next.getId() + " to remove list for any");
			remove.add(next);
		}

		// racks
		if (getProjectionData() != null && getProjectionData().getRacks() != null) {

			// make a copy of existing hosts backed by counts
			List<String> rackCountRacks = new ArrayList<String>(getRackCountHosts());

			for (String phost : getProjectionData().getRacks().keySet()) {
				rackCountRacks.remove(phost);
				Integer target = getProjectionData().getRacks().get(phost);
				delta = target - getRackCount(phost);
				data.getAllocateData().addRacks(phost, Math.max(delta, 0));
				removeCount = Math.max(-delta, 0);

				// wipe out nodes if ramp down happened
				for (String chost : getRackCountHosts()) {
					iterator = getRackCountMembers(chost).iterator();
					while (iterator.hasNext() && removeCount-- > 0) {
						remove.add(iterator.next());
					}
				}
			}

			// wipe out remaining node not tracked anymore
			for (String rhost : rackCountRacks) {
				iterator = getRackCountMembers(rhost).iterator();
				while (iterator.hasNext()) {
					remove.add(iterator.next());
				}
			}

		}

		// hosts
		if (getProjectionData() != null && getProjectionData().getHosts() != null) {
			// make a copy of existing hosts backed by counts
			List<String> hostCountHosts = new ArrayList<String>(getHostCountHosts());

			for (String phost : getProjectionData().getHosts().keySet()) {
				hostCountHosts.remove(phost);
				Integer target = getProjectionData().getHosts().get(phost);
				delta = target - getHostCount(phost);
				data.getAllocateData().addHosts(phost, Math.max(delta, 0));
				removeCount = Math.max(-delta, 0);
				log.debug("About to remove " + removeCount + " containers from " + phost);

				// wipe out nodes if ramp down happened
				iterator = getHostCountMembers(phost).iterator();
				while (iterator.hasNext() && removeCount-- > 0) {
					GridMember next = iterator.next();
					log.debug("Adding " + next.getId() + " to remove list for " + phost);
					remove.add(next);
				}

			}

			// wipe out remaining node not tracked anymore
			for (String rhost : hostCountHosts) {
				iterator = getHostCountMembers(rhost).iterator();
				while (iterator.hasNext()) {
					GridMember next = iterator.next();
					log.debug("Adding " + next.getId() + " to remove list for " + rhost);
					remove.add(next);
				}
			}
		}

		data.setRemoveData(remove);
		return data;
	}

	@Override
	public boolean acceptMember(GridMember member) {
		// we need to have a same priority
		if (!canFit(member)) {
			return false;
		}
		boolean added = false;

		if (added = tryHostAccept(member)) {
			return true;
		}

		if (added = tryRackAccept(member)) {
			return true;
		}

		if (added = tryAnyAccept(member)) {
			return true;
		}

		if (!added && !locality) {
			log.info("trying to force host/rack accepts");
			if (forceHostAccept(member)) {
				return true;
			}
			if (forceRackAccept(member)) {
				return true;
			}
		}


		return added;
	}

	/**
	 * Instantiates a new default grid projection.
	 *
	 * @param configuration the configuration
	 */
	public DefaultGridProjection(Configuration configuration) {
		super(configuration);
	}

	public void setLocality(boolean locality) {
		this.locality = locality;
	}

	private boolean tryHostAccept(GridMember member) {
		String host = member.getContainer().getNodeId().getHost();
		Collection<GridMember> hostCountMembers = getHostCountMembers(host);
		Integer target = getProjectionData().getHosts().get(host);
		if (target != null && hostCountMembers.size() < target) {
			if (addHostMember(member)) {
				return true;
			}
		}
		return false;
	}

	private boolean forceHostAccept(GridMember member) {
		for (String host : getHostCountHosts()) {
			Collection<GridMember> hostCountMembers = getHostCountMembers(host);
			Integer target = getProjectionData().getHosts().get(host);
			if (hostCountMembers.size() < target) {
				addHostCountMember(host, member);
				return true;
			}
		}
		return false;
	}

	private boolean forceRackAccept(GridMember member) {
		for (String host : getRackCountHosts()) {
			Collection<GridMember> rackCountMembers = getRackCountMembers(host);
			Integer target = getProjectionData().getRacks().get(host);
			if (rackCountMembers.size() < target) {
				addRackCountMember(host, member);
				return true;
			}
		}
		return false;
	}

	private boolean tryRackAccept(GridMember member) {
		if (getConfiguration() != null) {
			String host = member.getContainer().getNodeId().getHost();
			String rack = RackResolver.resolve(getConfiguration(), host).getNetworkLocation();
			if (rack == null) {
				log.warn("Failed to resolve rack for node " + host + ".");
			} else {
				log.info("Resolve rack for node " + host + " into " + rack);
				Integer target = getProjectionData().getRacks().get(rack);
				Collection<GridMember> rackCountMembers = getRackCountMembers(rack);
				if (rackCountMembers != null && target != null && rackCountMembers.size() < target) {
					if (addRackMember(member)) {
						return true;
					}
				}
			}
		} else {
			log.warn("Failed to resolve rack for node - no configuration");
		}
		return false;
	}

	private boolean tryAnyAccept(GridMember member) {
		Integer target = (getProjectionData() != null ? getProjectionData().getAny() : 0);
		if (getAnyCountMembers().size() < target) {
			if (addAnyMember(member)) {
				return true;
			}
		}
		return false;
	}

}
