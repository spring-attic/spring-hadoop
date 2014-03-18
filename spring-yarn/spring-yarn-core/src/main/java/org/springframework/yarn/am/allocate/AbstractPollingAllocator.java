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
package org.springframework.yarn.am.allocate;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.util.Assert;
import org.springframework.yarn.YarnSystemException;
import org.springframework.yarn.support.compat.NMTokenCacheCompat;

/**
 * Base implementation of allocator which is meant to handle
 * allocation by doing a simple periodic polling against
 * resource manager.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractPollingAllocator extends AbstractAllocator {

	private static final Log log = LogFactory.getLog(AbstractAllocator.class);

	/** Trigger for polling task */
	private volatile Trigger trigger = new PeriodicTrigger(1000);

	/** Poller runnable  */
	private volatile Runnable poller;

	/** Current running task if any */
	private volatile ScheduledFuture<?> runningTask;

	/**
	 * Sets {@link Trigger} used to trigger polling tasks.
	 *
	 * @param trigger trigger to set
	 */
	public void setTrigger(Trigger trigger) {
		Assert.notNull(trigger, "trigger must not be null");
		this.trigger = trigger;
	}

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		Assert.notNull(trigger, "Trigger is required");
		try {
			this.poller = this.createPoller();
		} catch (Exception e) {
			throw new YarnSystemException("Failed to create Poller", e);
		}
	}

	@Override
	protected void doStart() {
		Assert.state(getTaskScheduler() != null, "unable to start polling, no taskScheduler available");
		this.runningTask = getTaskScheduler().schedule(this.poller, this.trigger);
	}

	@Override
	protected void doStop() {
		if (this.runningTask != null) {
			this.runningTask.cancel(true);
		}
		this.runningTask = null;
	}

	/**
	 * Subclasses needs to implements this method to do container
	 * requests against resource manager. This method is called
	 * during the polling cycle handled by this class. New containers
	 * and newly released containers are passed to methods
	 * {@link #handleAllocatedContainers(List)} and
	 * {@link #handleCompletedContainers(List)}.
	 *
	 * @return {@link AllocateResponse} from a resource manager
	 */
	protected abstract AllocateResponse doContainerRequest();

	/**
	 * Pre-process allocated containers. Allows implementors to
	 * intercept containers before further processing is done.
	 * Default implementation returns list as it is.
	 *
	 * @param containers the containers
	 * @return the list of containers
	 */
	protected List<Container> preProcessAllocatedContainers(List<Container> containers) {
		return containers;
	}

	/**
	 * Subclasses needs to implement this method to handle newly
	 * allocated containers.
	 *
	 * @param containers list of newly allocate containers
	 */
	protected abstract void handleAllocatedContainers(List<Container> containers);

	/**
	 * Subclasses needs to implement this method to handle newly
	 * released containers.
	 *
	 * @param containerStatuses list of newly released containers
	 */
	protected abstract void handleCompletedContainers(List<ContainerStatus> containerStatuses);

	/**
	 * Creates a poller runnable used with task execution.
	 *
	 * @return the poller runnable
	 */
	private Runnable createPoller() {
		Callable<Boolean> pollingTask = new Callable<Boolean>() {
			public Boolean call() throws Exception {
				return doPoll();
			}
		};
		return new Poller(pollingTask);
	}

	/**
	 * Contains the logic to do the actual polling.
	 *
	 * @return True if this poll operation did something, False otherwise
	 */
	private boolean doPoll() {
		boolean result = false;

		if (log.isDebugEnabled()){
			log.debug("Checking if we can poll new and completed containers.");
		}

		// we use application attempt id as a flag
		// to know when appmaster has done registration
		if(getApplicationAttemptId() == null) {
			if (log.isDebugEnabled()){
				log.debug("ApplicationAttemptId not set, delaying poll requests.");
			}
			return result;
		}

		AllocateResponse response = doContainerRequest();

		// for now just stash tokens into hadoops NMTokenCache
		if (!response.getNMTokens().isEmpty()) {
			populateNmTokenCache(response);
		}

		List<Container> allocatedContainers = preProcessAllocatedContainers(response.getAllocatedContainers());
		if(allocatedContainers != null && allocatedContainers.size() > 0) {
			if (log.isDebugEnabled()){
				log.debug("response has " + allocatedContainers.size() + " new containers");
				for (Container c : allocatedContainers) {
					log.debug("new container: " + c.getId());
				}
			}
			handleAllocatedContainers(allocatedContainers);
			if(getYarnEventPublisher() != null) {
				for(Container container : allocatedContainers) {
					getYarnEventPublisher().publishContainerAllocated(this, container);
				}
			}
			result = true;
		}

		List<ContainerStatus> containerStatuses = response.getCompletedContainersStatuses();
		if(containerStatuses != null && containerStatuses.size() > 0) {
			if (log.isDebugEnabled()) {
				log.debug("response has " + containerStatuses.size() + " completed containers");
				for (ContainerStatus cs : containerStatuses) {
					log.debug("completed container: " + cs.getContainerId() + " with status=" + cs);
				}
			}
			handleCompletedContainers(containerStatuses);
			if(getYarnEventPublisher() != null) {
				for(ContainerStatus containerStatus : containerStatuses) {
					getYarnEventPublisher().publishContainerCompleted(this, containerStatus);
				}
			}
			result = true;
		}

		return result;
	}

	/**
	 * Populate node manager token cache in {@link NMTokenCache}.
	 *
	 * @param allocateResponse the allocate response
	 */
	@SuppressWarnings("static-access")
	protected void populateNmTokenCache(AllocateResponse allocateResponse) {
		// In some distros NMTokenCache don't have static methods
		// so it's ok to suppress warnings for it, and anyway
		// we need to stay compatible
		NMTokenCache tokenCache = NMTokenCacheCompat.getNMTokenCache();
		for (NMToken token : allocateResponse.getNMTokens()) {
			String nodeId = token.getNodeId().toString();
			if (log.isDebugEnabled()) {
				log.info("Token from allocateResponse token=" + token);
				if (NMTokenCacheCompat.containsToken(tokenCache, nodeId)) {
					log.debug("Replacing token for : " + nodeId);
				} else {
					log.debug("Received new token for : " + nodeId);
				}
			}
			tokenCache.setNMToken(nodeId, token.getToken());
		}
	}

	/**
	 * Internal helper class for poller.
	 */
	private class Poller implements Runnable {

		private final Callable<Boolean> pollingTask;

		public Poller(Callable<Boolean> pollingTask) {
			this.pollingTask = pollingTask;
		}

		public void run() {
			getTaskExecutor().execute(new Runnable() {
				public void run() {
					try {
						// TODO: we could use boolean return value to do
						//       something productive like throttling polls
						pollingTask.call();
					} catch (Exception e) {
						throw new RuntimeException("Error executing polling task", e);
					}
				}
			});
		}
	}

}
