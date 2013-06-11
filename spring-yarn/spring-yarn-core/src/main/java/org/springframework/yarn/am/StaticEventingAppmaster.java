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
package org.springframework.yarn.am;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.yarn.am.allocate.AbstractAllocator;

/**
 * A simple application master implementation which will allocate
 * and launch a number of containers, monitor container statuses
 * and finally exit the application by sending corresponding
 * message back to resource manager. This implementation also
 * is able to handle failed containers.
 *
 * @author Janne Valkealahti
 *
 */
public class StaticEventingAppmaster extends AbstractEventingAppmaster implements YarnAppmaster {

	private static final Log log = LogFactory.getLog(StaticEventingAppmaster.class);

	/** Static count of containers to run */
	private int containerCount;

	@Override
	public void submitApplication() {
		log.info("Submitting application");
		registerAppmaster();
		start();
		if(getAllocator() instanceof AbstractAllocator) {
			((AbstractAllocator)getAllocator()).setApplicationAttemptId(getApplicationAttemptId());
		}
		containerCount = Integer.parseInt(getParameters().getProperty(AppmasterConstants.CONTAINER_COUNT, "1"));
		log.info("count: " + containerCount);
		getAllocator().allocateContainers(containerCount);
	}

	@Override
	protected void onContainerAllocated(Container container) {
		getMonitor().addContainer(container);
		getLauncher().launchContainer(container, getCommands());
	}

	@Override
	protected void onContainerLaunched(Container container) {
		getMonitor().reportContainer(container);
	}

	@Override
	protected void onContainerCompleted(ContainerStatus status) {
		super.onContainerCompleted(status);

		getMonitor().monitorContainer(status);

		int exitStatus = status.getExitStatus();
		ContainerId containerId = status.getContainerId();

		boolean handled = false;
		if (exitStatus > 0) {
			handled = onContainerFailed(containerId);
			if (!handled) {
				setFinalApplicationStatus(FinalApplicationStatus.FAILED);
				notifyCompleted();
			}
		} else {
			if (isComplete()) {
				notifyCompleted();
			}
		}
	}

	/**
	 * Called if completed container has failed. User
	 * may override this method to process failed container,
	 * i.e. making a request to re-allocate new container istead
	 * of failing the application.
	 *
	 * @param containerId the container id
	 * @return true, if container was handled.
	 */
	protected boolean onContainerFailed(ContainerId containerId) {
		return false;
	}

	/**
	 * Returns state telling if application is considered
	 * as complete. Default implementation is delegating
	 * call to container monitor.
	 *
	 * @return True if application is complete
	 */
	protected boolean isComplete() {
		return getMonitor().hasRunning();
	}

}
