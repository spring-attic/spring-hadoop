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
package org.springframework.yarn.batch.am;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.util.RackResolver;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.AbstractEventingAppmaster;
import org.springframework.yarn.am.AppmasterService;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.allocate.ContainerAllocateData;
import org.springframework.yarn.am.container.AbstractLauncher;
import org.springframework.yarn.am.container.ContainerRequestHint;
import org.springframework.yarn.batch.event.PartitionedStepExecutionEvent;
import org.springframework.yarn.batch.listener.CompositePartitionedStepExecutionStateListener;
import org.springframework.yarn.batch.listener.PartitionedStepExecutionStateListener;
import org.springframework.yarn.batch.listener.PartitionedStepExecutionStateListener.PartitionedStepExecutionState;
import org.springframework.yarn.batch.support.YarnJobLauncher;

/**
 * Base application master for running batch jobs.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractBatchAppmaster extends AbstractEventingAppmaster implements BatchYarnAppmaster, ContainerLauncherInterceptor {

	private static final Log log = LogFactory.getLog(AbstractBatchAppmaster.class);

	/** Yarn specific job launcher */
	private YarnJobLauncher yarnJobLauncher;

	/** Step executions as reported back from containers */
	private List<StepExecution> stepExecutions = new ArrayList<StepExecution>();

	/** Mapping parent to its child executions */
	private Map<StepExecution, Set<StepExecution>> masterExecutions = new HashMap<StepExecution, Set<StepExecution>>();

	/** Extra request data as hints */
	private Map<StepExecution, ContainerRequestHint> requestData = new LinkedHashMap<StepExecution, ContainerRequestHint>();

	/** Remote step names for step executions */
	private Map<StepExecution, String> remoteStepNames = new HashMap<StepExecution, String>();

	/** Mapping containers to assigned executions */
	private Map<ContainerId, StepExecution> containerToStepMap = new HashMap<ContainerId, StepExecution>();

	/** Listener for partitioned step execution statuses */
	private CompositePartitionedStepExecutionStateListener stepExecutionStateListener =
			new CompositePartitionedStepExecutionStateListener();

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		if(getLauncher() instanceof AbstractLauncher) {
			((AbstractLauncher)getLauncher()).addInterceptor(this);
		}
		RackResolver.init(getConfiguration());
	}

	@Override
	protected void onContainerAllocated(Container container) {
		if (log.isDebugEnabled()) {
			log.debug("Container allocated: " + container);
		}

		StepExecution stepExecution = null;

		String host = container.getNodeId().getHost();
		String rack = RackResolver.resolve(host).getNetworkLocation();
		if (log.isDebugEnabled()) {
			log.debug("Matching against host=" + host + " rack=" + rack);
		}

		Iterator<Entry<StepExecution, ContainerRequestHint>> iterator = requestData.entrySet().iterator();
		while (iterator.hasNext() && stepExecution == null) {
			Entry<StepExecution, ContainerRequestHint> entry = iterator.next();
			if (entry.getValue() != null && entry.getValue().getHosts() != null) {
				for (String h : entry.getValue().getHosts()) {
					if (h.equals(host)) {
						stepExecution = entry.getKey();
						break;
					}
				}
			}
		}
		log.debug("stepExecution after hosts match: " + stepExecution);

		iterator = requestData.entrySet().iterator();
		while (iterator.hasNext() && stepExecution == null) {
			Entry<StepExecution, ContainerRequestHint> entry = iterator.next();
			if (entry.getValue() != null && entry.getValue().getRacks() != null) {
				for (String r : entry.getValue().getRacks()) {
					if (r.equals(rack)) {
						stepExecution = entry.getKey();
						break;
					}
				}
			}
		}

		log.debug("stepExecution after racks match: " + stepExecution);

		iterator = requestData.entrySet().iterator();
		if (stepExecution == null && iterator.hasNext()) {
			stepExecution = iterator.next().getKey();
		}

		if (stepExecution != null) {
			requestData.remove(stepExecution);
			containerToStepMap.put(container.getId(), stepExecution);
			getLauncher().launchContainer(container, getCommands());
		} else {
			getAllocator().releaseContainer(container.getId());
		}
	}

	@Override
	protected void onContainerLaunched(Container container) {
		if (log.isDebugEnabled()) {
			log.debug("Container launched: " + container);
		}
	}

	@Override
	protected void onContainerCompleted(ContainerStatus status) {
		super.onContainerCompleted(status);

		// find assigned container for step execution
		ContainerId containerId = status.getContainerId();
		StepExecution stepExecution = containerToStepMap.get(containerId);

		if (stepExecution != null) {
			for (Entry<StepExecution, Set<StepExecution>> entry : masterExecutions.entrySet()) {
				Set<StepExecution> set = entry.getValue();
				if (set.remove(stepExecution)) {
					if (log.isDebugEnabled()) {
						log.debug("stepExecution=" + stepExecution + " removed");
					}
					// modified, but it back
					masterExecutions.put(entry.getKey(), set);
				}
				if (set.size() == 0) {
					// we consumed all executions, send complete event
					// TODO: we could track failures
					getYarnEventPublisher().publishEvent(new PartitionedStepExecutionEvent(this, entry.getKey()));
					stepExecutionStateListener.state(PartitionedStepExecutionState.COMPLETED, entry.getKey());
				}
			}
		} else {
			log.warn("No assigned step execution for containerId=" + containerId);
		}

		// finally notify allocator for release
		getAllocator().releaseContainer(containerId);
	}

	@Override
	public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
		AppmasterService service = getAppmasterService();

		if(log.isDebugEnabled()) {
			log.debug("Intercept launch context: " + context);
		}

		StepExecution stepExecution = containerToStepMap.get(container.getId());
		String jobName = remoteStepNames.get(stepExecution);

		if(service != null) {
			int port = service.getPort();
			String address = service.getHost();
			Map<String, String> env = new HashMap<String, String>(context.getEnvironment());
			env.put(YarnSystemConstants.FS_ADDRESS, getConfiguration().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
			env.put(YarnSystemConstants.AMSERVICE_PORT, Integer.toString(port));
			env.put(YarnSystemConstants.AMSERVICE_HOST, address);
			env.put(YarnSystemConstants.AMSERVICE_BATCH_STEPNAME, jobName);
			env.put(YarnSystemConstants.AMSERVICE_BATCH_STEPNAME, jobName);
			env.put(YarnSystemConstants.AMSERVICE_BATCH_STEPEXECUTIONNAME, stepExecution.getStepName());
			env.put(YarnSystemConstants.AMSERVICE_BATCH_JOBEXECUTIONID, Long.toString(stepExecution.getJobExecutionId()));
			env.put(YarnSystemConstants.AMSERVICE_BATCH_STEPEXECUTIONID, Long.toString(stepExecution.getId()));
			context.setEnvironment(env);
			return context;
		} else {
			return context;
		}
	}

	@Autowired(required = false)
	public void setYarnJobLauncher(YarnJobLauncher yarnJobLauncher) {
		this.yarnJobLauncher = yarnJobLauncher;
	}

	public YarnJobLauncher getYarnJobLauncher() {
		return yarnJobLauncher;
	}

	/**
	 * Adds the partitioned step execution state listener.
	 *
	 * @param listener the listener
	 */
	public void addPartitionedStepExecutionStateListener(PartitionedStepExecutionStateListener listener) {
		stepExecutionStateListener.register(listener);
	}

	/**
	 * Gets the step executions.
	 *
	 * @return the step executions
	 */
	public List<StepExecution> getStepExecutions() {
		return stepExecutions;
	}


	/**
	 * Adds the step splits.
	 *
	 * @param masterStepExecution the partitioned steps parent step execution
	 * @param remoteStepName the remote step name
	 * @param stepExecutions the step executions splits
	 * @param resourceRequests the request data for step executions
	 */
	public void addStepSplits(StepExecution masterStepExecution, String remoteStepName,
			Set<StepExecution> stepExecutions, Map<StepExecution, ContainerRequestHint> resourceRequests) {

		// from request data we get hints where container should be run.
		// find a well distributed union of hosts.
		ContainerAllocateData containerAllocateData = new ContainerAllocateData();
		int countNeeded = 0;
		HashSet<String> hostUnion = new HashSet<String>();
		for (Entry<StepExecution, ContainerRequestHint> entry : resourceRequests.entrySet()) {
			StepExecution se = entry.getKey();
			ContainerRequestHint crd = entry.getValue();

			requestData.put(se, crd);
			remoteStepNames.put(se, remoteStepName);

			countNeeded++;
			for (String host : crd.getHosts()) {
				hostUnion.add(host);
			}
		}

		while (countNeeded > 0) {
			Iterator<String> iterator = hostUnion.iterator();
			while (countNeeded > 0 && iterator.hasNext()) {
				String host = iterator.next();
				containerAllocateData.addHosts(host, 1);
				countNeeded--;
			}
		}

		if (log.isDebugEnabled()) {
			log.debug("Adding " + stepExecutions.size() + " split steps into masterStepExecution=" + masterStepExecution);
		}

		// Create new set due to SHDP-188
		HashSet<StepExecution> set = new HashSet<StepExecution>(stepExecutions.size());
		set.addAll(stepExecutions);
		masterExecutions.put(masterStepExecution, set);

		int remaining = stepExecutions.size() - resourceRequests.size();
		for (StepExecution execution : set) {
			if (!requestData.containsKey(execution)) {
				requestData.put(execution, null);
			}
			if (!remoteStepNames.containsKey(execution)) {
				remoteStepNames.put(execution, remoteStepName);
			}
		}

		getAllocator().allocateContainers(remaining);
		getAllocator().allocateContainers(containerAllocateData);
	}

}
