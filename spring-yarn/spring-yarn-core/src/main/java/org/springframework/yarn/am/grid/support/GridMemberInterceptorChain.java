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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.yarn.am.grid.Grid;
import org.springframework.yarn.am.grid.GridMember;

public class GridMemberInterceptorChain {

	private static final Log log = LogFactory.getLog(GridMemberInterceptorChain.class);

	private final List<GridMemberInterceptor> interceptors = new CopyOnWriteArrayList<GridMemberInterceptor>();

	public boolean set(List<GridMemberInterceptor> interceptors) {
		synchronized (this.interceptors) {
			this.interceptors.clear();
			return this.interceptors.addAll(interceptors);
		}
	}

	public boolean add(GridMemberInterceptor interceptor) {
		return this.interceptors.add(interceptor);
	}

	public List<GridMemberInterceptor> getInterceptors() {
		return Collections.unmodifiableList(this.interceptors);
	}

	public GridMember preAdd(GridMember member, Grid grid) {

		// if we don't have interceptors just pass
		// through grid member
		if (interceptors.isEmpty()) {
			return member;
		}

		for (GridMemberInterceptor interceptor : interceptors) {
			if (interceptor.preAdd(member, grid) != null) {
				return member;
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning null from preAdd because all interceptors returned null");
		}
		return null;
	}

}
