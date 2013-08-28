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
package org.springframework.yarn.am.container;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.util.Assert;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.YarnSystemException;

/**
 * Default container launcher.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultContainerLauncher extends AbstractLauncher implements ContainerLauncher {

	private final static Log log = LogFactory.getLog(DefaultContainerLauncher.class);

	private Set<Container> launched = new HashSet<Container>();

	/** Trigger for polling task */
	private volatile Trigger trigger = new PeriodicTrigger(5000);

	/** Poller runnable  */
	private volatile Runnable poller;

	/** Current running task if any */
	private volatile ScheduledFuture<?> runningTask;

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

	@Override
	public void launchContainer(Container container, List<String> commands) {
		if (log.isDebugEnabled()) {
			log.debug("Launching container: " + container);
		}

		ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
		ctx.setContainerId(container.getId());
		ctx.setResource(container.getResource());
		ctx.setUser(getUsername());
		String stagingId = Integer.toString(container.getId().getApplicationAttemptId().getApplicationId().getId());
		getResourceLocalizer().setStagingId(stagingId);
		ctx.setLocalResources(getResourceLocalizer().getResources());
		ctx.setCommands(commands);

		// Yarn doesn't tell container what is its container id
		// so we do it here
		Map<String, String> env = getEnvironment();
		env.put(YarnSystemConstants.SYARN_CONTAINER_ID, ConverterUtils.toString(container.getId()));
		ctx.setEnvironment(env);
		ctx = getInterceptors().preLaunch(ctx);

		StartContainerRequest request = Records.newRecord(StartContainerRequest.class);
		request.setContainerLaunchContext(ctx);
		getCmTemplate(container).startContainer(request);

		// notify interested parties of new launched container
		if(getYarnEventPublisher() != null) {
			getYarnEventPublisher().publishContainerLaunched(this, container);
		}
	}

	/**
	 * Contains the logic to do the actual polling.
	 *
	 * @return True if this poll operation did something, False otherwise
	 */
	private boolean doPoll() {
		boolean result = false;

		if (log.isDebugEnabled()) {
			log.debug("Checking status of containers previousely launched");
		}

		for (Iterator<Container> iterator = launched.iterator(); iterator.hasNext();) {
			Container container = iterator.next();
			ContainerStatus status = getCmTemplate(container).getContainerStatus();
			ContainerState state = status.getState();
			if (state.equals(ContainerState.COMPLETE)) {
				iterator.remove();
			} else if (state.equals(ContainerState.RUNNING)) {
				iterator.remove();
				if (getYarnEventPublisher() != null) {
					getYarnEventPublisher().publishContainerLaunched(this, container);
				}
			}
		}

		return result;
	}

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
