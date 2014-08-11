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

public class RacksGridProjection extends AbstractGridProjection {

	private static final Log log = LogFactory.getLog(RacksGridProjection.class);

	public RacksGridProjection() {
		super();
	}

	public RacksGridProjection(Configuration configuration) {
		super(configuration);
	}

	@Override
	public SatisfyStateData getSatisfyState() {
		SatisfyStateData data = new SatisfyStateData();
		ArrayList<GridMember> remove = new ArrayList<GridMember>();

		if (getProjectionData() != null && getProjectionData().getRacks() != null) {

			// make a copy of existing hosts backed by counts
			List<String> rackCountRacks = new ArrayList<String>(getRackCountHosts());

			for (String phost : getProjectionData().getRacks().keySet()) {
				rackCountRacks.remove(phost);
				Integer target = getProjectionData().getRacks().get(phost);
				int delta = target - getRackCount(phost);
				data.getAllocateData().addRacks(phost, Math.max(delta, 0));
				int removeCount = Math.max(-delta, 0);

				// wipe out nodes if ramp down happened
				for (String chost : getRackCountHosts()) {
					Iterator<GridMember> iterator = getRackCountMembers(chost).iterator();
					while (iterator.hasNext() && removeCount-- > 0) {
						remove.add(iterator.next());
					}
				}
			}

			// wipe out remaining node not tracked anymore
			for (String rhost : rackCountRacks) {
				Iterator<GridMember> iterator = getRackCountMembers(rhost).iterator();
				while (iterator.hasNext()) {
					remove.add(iterator.next());
				}
			}

		}
		data.setRemoveData(remove);
		return data;
	}

	@Override
	public boolean acceptMember(GridMember member) {

		if (!isSamePriority(member)) {
			return false;
		}

		String host = member.getContainer().getNodeId().getHost();
		Collection<GridMember> hostCountMembers = getHostCountMembers(host);

		Integer target = null;
		if (getConfiguration() != null) {
			String rack = RackResolver.resolve(getConfiguration(), host).getNetworkLocation();
			if (rack == null) {
				log.warn("Failed to resolve rack for node " + host + ".");
			} else {
				log.info("Resolve rack for node " + host + " into " + rack);
				target = getProjectionData().getRacks().get(rack);
			}
		} else {
			log.warn("Failed to resolve rack for node - no configuration");
		}


		if (target == null) {
			return false;
		}

		if (hostCountMembers.size() < target) {
			return addMember(member);
		} else {
			return false;
		}
	}

}
