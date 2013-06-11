/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.am.monitor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.springframework.yarn.listener.ContainerMonitorListener.ContainerMonitorState;

/**
 * Default implementation of {@link ContainerMonitor} which simple
 * tracks number of total and completed containers.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultContainerMonitor extends AbstractMonitor implements ContainerMonitor {

	/** Containers which has been allocated */
	private Set<ContainerId> allocated = new HashSet<ContainerId>();

	/** Containers are currently running */
	private Set<ContainerId> running = new HashSet<ContainerId>();

	/** Containers which has been completed */
	private Set<ContainerId> completed = new HashSet<ContainerId>();

	/** Containers which has been completed with failed status */
	private Set<ContainerId> failed = new HashSet<ContainerId>();

	@Override
	public void monitorContainer(List<ContainerStatus> completedContainers) {
		for (ContainerStatus status : completedContainers) {
			ContainerId containerId = status.getContainerId();
			int exitStatus = status.getExitStatus();
			ContainerState state = status.getState();

			if (state.equals(ContainerState.COMPLETE)) {
				if (exitStatus > 0) {
					failed.add(containerId);
				} else {
					completed.add(containerId);
				}
			}
			running.remove(containerId);
		}
		int total = allocated.size();
		int fail = failed.size();
		int comp = completed.size()+fail;
		notifyState(new ContainerMonitorState(total, comp, fail, comp/(double)total));
	}

	@Override
	public void monitorContainer(ContainerStatus completedContainer) {
		ContainerId containerId = completedContainer.getContainerId();
		int exitStatus = completedContainer.getExitStatus();
		ContainerState state = completedContainer.getState();

		if (state.equals(ContainerState.COMPLETE)) {
			if (exitStatus > 0) {
				failed.add(containerId);
			} else {
				completed.add(containerId);
			}
		}
		running.remove(containerId);
		int total = allocated.size();
		int fail = failed.size();
		int comp = completed.size()+fail;
		notifyState(new ContainerMonitorState(total, comp, fail, comp/(double)total));
	}

	@Override
	public void reportContainer(Container container) {
		if (container.getState().equals(ContainerState.NEW)) {
			allocated.add(container.getId());
		}
		if (container.getState().equals(ContainerState.RUNNING)) {
			running.add(container.getId());
			allocated.remove(container.getId());
		}
	}

	@Override
	public void addContainer(Container container) {
		allocated.add(container.getId());
	}

	@Override
	public boolean hasRunning() {
		return !running.isEmpty();
	}

	@Override
	public boolean hasFailed() {
		return !failed.isEmpty();
	}

	@Override
	public boolean hasFree() {
		return !allocated.isEmpty();
	}

}
