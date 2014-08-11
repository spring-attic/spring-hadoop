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
import java.util.Iterator;

import org.springframework.yarn.am.grid.GridMember;
import org.springframework.yarn.am.grid.GridProjection;


/**
 * {@link GridProjection} which accepts any node for its members.
 *
 * @author Janne Valkealahti
 *
 */
public class AnyGridProjection extends AbstractGridProjection {

	private int count;

	public AnyGridProjection() {
		super();
	}

	public AnyGridProjection(int count) {
		super();
		this.count = count;
	}

	@Override
	public SatisfyStateData getSatisfyState() {
		SatisfyStateData data = new SatisfyStateData();

		// simply add delta for current size vs. requested count
		int delta = count - getMembers().size();
		data.getAllocateData().addAny(Math.max(delta, 0));

		// Simply remove using negative delta
		int removeCount = Math.max(-delta, 0);
		Iterator<GridMember> iterator = getMembers().iterator();
		ArrayList<GridMember> remove = new ArrayList<GridMember>();
		while (iterator.hasNext() && removeCount-- > 0) {
			remove.add(iterator.next());
		}
		data.setRemoveData(remove);

		return data;
	}

	@Override
	public void setProjectionData(ProjectionData data) {
		super.setProjectionData(data);
		count = data.getAny() != null ? data.getAny() : 0;
	}

	@Override
	public boolean acceptMember(GridMember member) {
		if (!isSamePriority(member)) {
			return false;
		}
		if (getMembers().size() < count) {
			return addMember(member);
		} else {
			return false;
		}
	}

}
