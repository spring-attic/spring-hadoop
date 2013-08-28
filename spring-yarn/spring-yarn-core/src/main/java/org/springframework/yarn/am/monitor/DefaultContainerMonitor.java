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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.yarn.listener.ContainerMonitorListener.ContainerMonitorState;

/**
 * Default implementation of {@link ContainerMonitor} which simple
 * tracks number of total and completed containers.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultContainerMonitor extends AbstractMonitor implements ContainerMonitor {

	private final static Log log = LogFactory.getLog(DefaultContainerMonitor.class);

	/**
	 * Containers which has been allocated. These are considered as free and
	 * we don't know if container is running or completed.
	 */
	private Set<String> allocated = new HashSet<String>();

	/** Containers which are currently running */
	private Set<String> running = new HashSet<String>();

	/** Containers which has been completed */
	private Set<String> completed = new HashSet<String>();

	/** Containers which has been completed with failed status */
	private Set<String> failed = new HashSet<String>();

	/** Lock for sets updates */
	private final Object lock = new Object();

	@Override
	public void reportContainerStatus(List<ContainerStatus> containerStatuses) {
		handleContainerStatus(containerStatuses, false);
	}

	@Override
	public void reportContainerStatus(ContainerStatus containerStatus) {
		List<ContainerStatus> containerStatuses = new ArrayList<ContainerStatus>();
		containerStatuses.add(containerStatus);
		handleContainerStatus(containerStatuses, false);
	}

	@Override
	public void reportContainer(Container container) {
		if (log.isDebugEnabled()) {
			log.debug("Reporting container=" + container);
		}
		String cid = ConverterUtils.toString(container.getId());
		synchronized (lock) {
			if (container.getState().equals(ContainerState.NEW)) {
				allocated.add(cid);
			} else if (container.getState().equals(ContainerState.RUNNING)) {
				running.add(cid);
				allocated.remove(cid);
			} else if (container.getState().equals(ContainerState.COMPLETE)) {
				running.remove(cid);
			} else {
				log.warn("Got unknown ContainerState=" + container.getState());
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("State after reportContainer: " + toDebugString());
		}
	}

	@Override
	public int freeCount() {
		return allocated.size();
	}

	@Override
	public int runningCount() {
		return running.size();
	}

	@Override
	public int failedCount() {
		return failed.size();
	}

	@Override
	public int completedCount() {
		return completed.size();
	}

	private void handleContainerStatus(List<ContainerStatus> containerStatuses, boolean notifyIntermediates) {
		for (ContainerStatus status : containerStatuses) {

			if (log.isDebugEnabled()) {
				log.debug("Reporting containerStatus=" + status);
			}

			ContainerId containerId = status.getContainerId();
			int exitStatus = status.getExitStatus();
			ContainerState state = status.getState();
			String cid = ConverterUtils.toString(containerId);

			synchronized (lock) {
				if (state.equals(ContainerState.COMPLETE)) {
					if (exitStatus > 0) {
						failed.add(cid);
					} else {
						completed.add(cid);
					}
				}
				allocated.remove(cid);
				running.remove(cid);
			}

			if (notifyIntermediates) {
				dispatchCurrentContainerMonitorState();
			}
		}
		if (!notifyIntermediates) {
			dispatchCurrentContainerMonitorState();
		}
		if (log.isDebugEnabled()) {
			log.debug("State after handleContainerStatus: " + toDebugString());
		}
	}

	/**
	 * Dispatches current {@link ContainerMonitorState} into event listener.
	 */
	private void dispatchCurrentContainerMonitorState() {
		int total = allocated.size();
		int fail = failed.size();
		int comp = completed.size()+fail;
		notifyState(new ContainerMonitorState(total, comp, fail, comp/(double)total));
	}

	/**
	 * Gets this class description as a debug string.
	 *
	 * @return class description as a debug string
	 */
	public String toDebugString() {
		return "DefaultContainerMonitor [allocated=" + toDebugStringContainerSet(allocated) + ", running="
				+ toDebugStringContainerSet(running) + ", completed=" + toDebugStringContainerSet(completed)
				+ ", failed=" + toDebugStringContainerSet(failed) + "]";
	}

	/**
	 * Helper method to debug content of sets in this class.
	 *
	 * @param set the set used in this class
	 * @return Set as debug string
	 */
	private String toDebugStringContainerSet(Set<String> set) {
		StringBuilder buf = new StringBuilder();
		buf.append('[');
		for (String containerId : set) {
			buf.append(containerId);
			buf.append(',');
		}
		buf.append(']');
		return buf.toString();
	}

}
