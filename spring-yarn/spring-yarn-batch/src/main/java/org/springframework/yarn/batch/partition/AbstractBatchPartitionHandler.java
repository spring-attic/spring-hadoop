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
package org.springframework.yarn.batch.partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.StepExecutionSplitter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.container.ContainerRequestHint;
import org.springframework.yarn.am.container.ContainerResolver;
import org.springframework.yarn.batch.am.BatchYarnAppmaster;
import org.springframework.yarn.batch.listener.PartitionedStepExecutionStateListener;
import org.springframework.yarn.listener.AppmasterStateListener;

/**
 * Base implementation of Spring Batch {@link PartitionHandler} handling
 * partitioning for Yarn containers.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractBatchPartitionHandler implements PartitionHandler {

	private static final Log log = LogFactory.getLog(AbstractBatchPartitionHandler.class);

	/** Application master used for batch operations */
	private BatchYarnAppmaster batchAppmaster;

	/** Default remote step name to execute */
	private String stepName = "remoteStep";

	/** Resolver for containers */
	private ContainerResolver containerResolver;

	/**
	 * Instantiates a new batch partition handler.
	 */
	public AbstractBatchPartitionHandler() {
		// for able to set appmaster via setter
	}

	/**
	 * Instantiates a new batch partition handler.
	 *
	 * @param batchAppmaster the batch appmaster
	 */
	public AbstractBatchPartitionHandler(BatchYarnAppmaster batchAppmaster) {
		this.batchAppmaster = batchAppmaster;
	}

	/**
	 * Sets the batch appmaster.
	 *
	 * @param batchAppmaster the new batch appmaster
	 */
	public void setBatchAppmaster(BatchYarnAppmaster batchAppmaster) {
		this.batchAppmaster = batchAppmaster;
	}

	@Autowired(required=false)
	public void setYarnAppmaster(YarnAppmaster yarnAppmaster) {
		if (yarnAppmaster instanceof BatchYarnAppmaster) {
			setBatchAppmaster((BatchYarnAppmaster) yarnAppmaster);
		}
	}

	protected abstract Set<StepExecution> createStepExecutionSplits(StepExecutionSplitter stepSplitter, StepExecution stepExecution)
			throws Exception;

	/**
	 * Subclass may override this method to assign a specific {@link ContainerRequestHint} to
	 * a {@link StepExecution}. This would be needed in cases where step should be executed
	 * in a specific host or rack considering data locality. Default implementation
	 * returns an empty map.
	 *
	 * @param stepExecutions Set of step executions
	 * @return Mapping between step executions and container request data
	 * @throws Exception If error occurred
	 */
	protected Map<StepExecution, ContainerRequestHint> createResourceRequestData(Set<StepExecution> stepExecutions)
			throws Exception {
		return new HashMap<StepExecution, ContainerRequestHint>();
	}

	@Override
	public final Collection<StepExecution> handle(StepExecutionSplitter stepSplitter, StepExecution stepExecution)
			throws Exception {

		if (log.isDebugEnabled()) {
			log.debug("partition job parameters:");
			for (Entry<String, JobParameter> entry : stepExecution.getJobParameters().getParameters().entrySet()) {
				log.debug("entry: " + entry.getKey() + " / " + entry.getValue());
			}
		}

		Collection<StepExecution> result = new ArrayList<StepExecution>();

		Set<StepExecution> split = createStepExecutionSplits(stepSplitter, stepExecution);

		if (log.isDebugEnabled()) {
			log.debug("Created " + split.size() + " splits for stepName=" + stepName +
					" with parent stepExecution=" + stepExecution);
			for (StepExecution execution : split) {
				log.debug("Splitted step execution: " + execution + " with executionContext=" + execution.getExecutionContext());
			}
		}

		Map<StepExecution, ContainerRequestHint> resourceRequests = createResourceRequestData(split);

		if (log.isDebugEnabled()) {
			log.debug("Resource request map size is " + resourceRequests.size());
			for (Entry<StepExecution, ContainerRequestHint> entry : resourceRequests.entrySet()) {
				log.debug("Entry stepExecution=" + entry.getKey() + " requestData=" + entry.getValue());
			}
		}

		batchAppmaster.addStepSplits(stepExecution, stepName, split, resourceRequests);

		waitCompleteState(stepExecution);

		result.addAll(batchAppmaster.getStepExecutions());
		if (log.isDebugEnabled()) {
			log.debug("Statuses of remote execution ");
			for (StepExecution execution : result) {
				log.debug("Remote step execution: " + execution);
			}
		}

		return result;
	}

	/**
	 * Gets the step name.
	 *
	 * @return the step name
	 */
	public String getStepName() {
		return stepName;
	}

	/**
	 * Sets the step name.
	 *
	 * @param stepName the new step name
	 */
	public void setStepName(String stepName) {
		this.stepName = stepName;
	}

	/**
	 * Gets the container resolver.
	 *
	 * @return the container resolver
	 */
	public ContainerResolver getContainerResolver() {
		return containerResolver;
	}

	/**
	 * Sets the container resolver.
	 *
	 * @param containerResolver the new container resolver
	 */
	public void setContainerResolver(ContainerResolver containerResolver) {
		this.containerResolver = containerResolver;
	}

	/**
	 * Uses {@link CountDownLatch} to wait completion status from
	 * application master. Status is considered to be complete if either
	 * master itself or parent step execution sends complete status.
	 *
	 * @param masterStepExecution the parent step execution
	 */
	protected void waitCompleteState(final StepExecution masterStepExecution) {
		final CountDownLatch latch = new CountDownLatch(1);

		// if we get complete for appmaster, bail out
		batchAppmaster.addAppmasterStateListener(new AppmasterStateListener() {
			@Override
			public void state(AppmasterState state) {
				if (log.isDebugEnabled()) {
					log.debug("AppmasterStateListener state: state=" + state);
				}
				if(state == AppmasterState.COMPLETED) {
					latch.countDown();
				}
			}
		});

		batchAppmaster.addPartitionedStepExecutionStateListener(new PartitionedStepExecutionStateListener() {
			@Override
			public void state(PartitionedStepExecutionState state, StepExecution stepExecution) {

				if (log.isDebugEnabled()) {
					log.debug("PartitionedStepExecutionStateListener state: state=" + state +
							" stepExecution=" + stepExecution + " masterStepExecution=" + masterStepExecution);
				}

				if(state == PartitionedStepExecutionState.COMPLETED && masterStepExecution.equals(stepExecution)) {
					if (log.isDebugEnabled()) {
						log.debug("Got complete state for stepExecution=" + stepExecution);
					}
					latch.countDown();
				}
			}
		});

		try {
			latch.await();
		} catch (Exception e) {
			log.warn("Latch wait interrupted, we may not be finished!");
		}
	}

}
