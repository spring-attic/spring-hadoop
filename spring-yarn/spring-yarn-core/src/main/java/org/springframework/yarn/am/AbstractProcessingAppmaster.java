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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.springframework.util.Assert;
import org.springframework.yarn.am.container.AbstractLauncher;
import org.springframework.yarn.am.monitor.ContainerAware;
import org.springframework.yarn.listener.ContainerAllocatorListener;

/**
 * Base application master implementation which handles a simple
 * life-cycle scenario of; allocate, launch, monitor.
 * <p>
 * We can say that the actual implementation of this is very static
 * in terms of what application master can do. Everything needs
 * to be known prior to starting the life-cycle. Implementation
 * should know how many containers will participate the application,
 * what those containers will do and what is the expected outcome
 * from a container execution.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractProcessingAppmaster extends AbstractServicesAppmaster implements ContainerLauncherInterceptor {

	private static final Log log = LogFactory.getLog(AbstractProcessingAppmaster.class);

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		Assert.notNull(getAllocator(), "Container allocator must be set");
		Assert.notNull(getLauncher(), "Container launcher must be set");
		Assert.notNull(getMonitor(), "Container monitor must be set");

		if(getLauncher() instanceof AbstractLauncher) {
			((AbstractLauncher)getLauncher()).addInterceptor(this);
		}

		if(log.isDebugEnabled()) {
			log.debug("Using handlers allocator=" + getAllocator() +
					" launcher=" + getLauncher() +
					" monitor=" + getMonitor());
		}

		// setting up internal dispatcher
		getAllocator().addListener(new ContainerAllocatorListener() {
			@Override
			public void allocated(List<Container> allocatedContainers) {
				for (Container container : allocatedContainers) {
					if (getMonitor() instanceof ContainerAware) {
						((ContainerAware)getMonitor()).onContainer(allocatedContainers);
					}
					getLauncher().launchContainer(container, getCommands());
					onContainerAllocated(container);
				}
			}
			@Override
			public void completed(List<ContainerStatus> completedContainers) {
				if (getMonitor() instanceof ContainerAware) {
					((ContainerAware)getMonitor()).onContainerStatus(completedContainers);
				}
				for (ContainerStatus status : completedContainers) {
					onContainerCompleted(status);
				}
			}
		});
	}

	@Override
	public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
		return context;
	}

	/**
	 * Called when container has been allocated. Default
	 * implementation is not doing anything.
	 *
	 * @param container the container
	 */
	protected void onContainerAllocated(Container container) {
	}

	/**
	 * Called when container has been completed. Default
	 * implementation is not doing anything.
	 *
	 * @param status the status
	 */
	protected void onContainerCompleted(ContainerStatus status) {
	}

}
