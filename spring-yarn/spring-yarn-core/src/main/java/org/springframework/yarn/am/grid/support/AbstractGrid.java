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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.springframework.util.Assert;
import org.springframework.yarn.am.grid.Grid;
import org.springframework.yarn.am.grid.GridMember;
import org.springframework.yarn.am.grid.listener.DefaultGridListener;
import org.springframework.yarn.am.grid.listener.GridListener;

/**
 * Simple {@code Grid} base implementation.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractGrid implements Grid {

	/** Listener dispatcher for grid events */
	private final DefaultGridListener gridListeners = new DefaultGridListener();

	private final GridMemberInterceptorChain interceptorChain = new GridMemberInterceptorChain();

	private ConcurrentHashMap<ContainerId, GridMember> members = new ConcurrentHashMap<ContainerId, GridMember>();

	@Override
	public Collection<GridMember> getMembers() {
		return members.values();
	}

	@Override
	public GridMember getMember(ContainerId id) {
		return members.get(id);
	}

	@Override
	public boolean addMember(GridMember node) {
		Assert.notNull(node, "Node must not be null");

		node = interceptorChain.preAdd(node, this);

		if (node != null && members.putIfAbsent(node.getId(), node) == null) {
			notifyMemberAdded(node);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean removeMember(ContainerId id) {
		Assert.notNull(id, "Node identifier must not be null");
		GridMember removed = members.remove(id);
		if (removed != null) {
			notifyMemberRemoved(removed);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void addGridListener(GridListener listener) {
		gridListeners.register(listener);
	}

	/**
	 * Set the list of channel interceptors. This will clear any existing interceptors.
	 *
	 * @param interceptors list of interceptors
	 */
	public void setInterceptors(List<GridMemberInterceptor> interceptors) {
		this.interceptorChain.set(interceptors);
	}

	/**
	 * Add a channel interceptor to the end of the list.
	 */
	@Override
	public void addInterceptor(GridMemberInterceptor interceptor) {
		this.interceptorChain.add(interceptor);
	}

	/**
	 * Return a read-only list of the configured interceptors.
	 *
	 * @return list of interceptors
	 */
	public List<GridMemberInterceptor> getInterceptors() {
		return this.interceptorChain.getInterceptors();
	}

	/**
	 * Exposes the interceptor chain for subclasses.
	 *
	 * @return interceptor chain
	 */
	protected GridMemberInterceptorChain getInterceptorChain() {
		return this.interceptorChain;
	}

	/**
	 * Notifies registered {@code ContainerGridListener}s that
	 * a {@code ContainerNode} has been added to a {@code ContainerGrid}.
	 *
	 * @param member the node
	 */
	protected void notifyMemberAdded(GridMember member) {
		gridListeners.memberAdded(member);
	}

	/**
	 * Notifies registered {@code ContainerGridListener}s that
	 * a {@code ContainerNode} has been removed from a {@code ContainerGrid}.
	 *
	 * @param member the node
	 */
	protected void notifyMemberRemoved(GridMember member) {
		gridListeners.memberRemoved(member);
	}

}
