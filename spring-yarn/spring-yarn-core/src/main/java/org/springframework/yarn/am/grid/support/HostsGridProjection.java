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
import org.springframework.yarn.am.grid.GridMember;

public class HostsGridProjection extends AbstractGridProjection {

	private static final Log log = LogFactory.getLog(HostsGridProjection.class);

	public HostsGridProjection() {
		super();
	}

	@Override
	public SatisfyStateData getSatisfyState() {
		SatisfyStateData data = new SatisfyStateData();
		ArrayList<GridMember> remove = new ArrayList<GridMember>();

		if (getProjectionData() != null && getProjectionData().getHosts() != null) {
			// make a copy of existing hosts backed by counts
			List<String> hostCountHosts = new ArrayList<String>(getHostCountHosts());

			for (String phost : getProjectionData().getHosts().keySet()) {
				hostCountHosts.remove(phost);
				Integer target = getProjectionData().getHosts().get(phost);
				int delta = target - getHostCount(phost);
				data.getAllocateData().addHosts(phost, Math.max(delta, 0));
				int removeCount = Math.max(-delta, 0);
				log.debug("About to remove " + removeCount + " containers from " + phost);

				// wipe out nodes if ramp down happened
				Iterator<GridMember> iterator = getHostCountMembers(phost).iterator();
				while (iterator.hasNext() && removeCount-- > 0) {
					GridMember next = iterator.next();
					log.debug("Adding " + next.getId() + " to remove list for " + phost);
					remove.add(next);
				}

			}

			// wipe out remaining node not tracked anymore
			for (String rhost : hostCountHosts) {
				Iterator<GridMember> iterator = getHostCountMembers(rhost).iterator();
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

		if (!isSamePriority(member)) {
			return false;
		}

		String host = member.getContainer().getNodeId().getHost();
		Collection<GridMember> hostCountMembers = getHostCountMembers(host);

		Integer target = getProjectionData().getHosts().get(host);
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
