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
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.StepExecutionSplitter;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.container.ContainerRequestHint;
import org.springframework.yarn.batch.BatchSystemConstants;
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
public abstract class AbstractPartitionHandler implements PartitionHandler {

	private static final Log log = LogFactory.getLog(AbstractPartitionHandler.class);

	private BatchYarnAppmaster batchAppmaster;

	private String stepName = "remoteStep";

	private String keySplitLocations = BatchSystemConstants.KEY_SPLITLOCATIONS;

	/**
	 * Instantiates a new abstract partition handler.
	 */
	public AbstractPartitionHandler() {
	}

	/**
	 * Instantiates a new abstract partition handler.
	 *
	 * @param batchAppmaster the batch appmaster
	 */
	public AbstractPartitionHandler(BatchYarnAppmaster batchAppmaster) {
		this.batchAppmaster = batchAppmaster;
	}

	@Override
	public final Collection<StepExecution> handle(StepExecutionSplitter stepSplitter, StepExecution stepExecution)
			throws Exception {
		log.info("Handling stepExecution=[" + stepExecution + "] with jobParameters=["
				+ stepExecution.getJobParameters() + "]");

		Set<StepExecution> split = createSplits(stepSplitter, stepExecution);
		log.info("Created " + split.size() + " splits for stepName=" + stepName);

		Map<StepExecution, ContainerRequestHint> resourceRequests = createRequestData(split);
		log.info("Resource request map size is " + resourceRequests.size());
		if (log.isDebugEnabled()) {
			for (Entry<StepExecution, ContainerRequestHint> entry : resourceRequests.entrySet()) {
				log.debug("Entry stepExecution=[" + entry.getKey() + "] requestData=[" + entry.getValue() + "]");
			}
		}

		batchAppmaster.addStepSplits(stepExecution, stepName, split, resourceRequests);
		waitCompleteState(stepExecution);
		Collection<StepExecution> result = new ArrayList<StepExecution>(batchAppmaster.getStepExecutions());

		log.info("Listing statuses of remote executions");
		for (StepExecution execution : result) {
			log.info("Remote stepExecution=[" + execution + "]");
		}
		return result;
	}

	/**
	 * Sets the batch appmaster.
	 *
	 * @param batchAppmaster the new batch appmaster
	 * @see #setYarnAppmaster(YarnAppmaster)
	 */
	public void setBatchAppmaster(BatchYarnAppmaster batchAppmaster) {
		this.batchAppmaster = batchAppmaster;
	}

	/**
	 * Sets the batch appmaster. This is a specific method
	 * using a {@link YarnAppmaster} to be able to allow
	 * application context to auto-wire {@link BatchYarnAppmaster}
	 * if it's type in context is defined as {@link YarnAppmaster}.
	 *
	 * @param yarnAppmaster the new yarn appmaster
	 * @see #setBatchAppmaster(BatchYarnAppmaster)
	 */
//	@Autowired(required=false)
	public void setYarnAppmaster(YarnAppmaster yarnAppmaster) {
		if (yarnAppmaster instanceof BatchYarnAppmaster) {
			setBatchAppmaster((BatchYarnAppmaster) yarnAppmaster);
		}
	}

	/**
	 * The name of the key for the split locations in each {@link ExecutionContext}.
	 * Defaults to "splitLocations".
	 *
	 * @param keySplitLocations the value of the key
	 */
	public void setKeySplitLocations(String keySplitLocations) {
		this.keySplitLocations = keySplitLocations;
	}

	/**
	 * Gets the key split locations.
	 *
	 * @return the key split locations
	 */
	public String getKeySplitLocations() {
		return keySplitLocations;
	}

	/**
	 * Gets the remote step name.
	 *
	 * @return the remote step name
	 */
	public String getStepName() {
		return stepName;
	}

	/**
	 * Sets the remote step name.
	 *
	 * @param stepName the new remote step name
	 */
	public void setStepName(String stepName) {
		this.stepName = stepName;
	}

	/**
	 * Creates the splits. Implementor needs to override
	 * this method to create an actual step splits.
	 *
	 * @param stepSplitter the step splitter
	 * @param stepExecution the step execution
	 * @return the step executions
	 * @throws Exception the exception
	 */
	protected abstract Set<StepExecution> createSplits(StepExecutionSplitter stepSplitter, StepExecution stepExecution)
			throws Exception;

	/**
	 * Subclass may override this method to assign a specific {@link ContainerRequestHint} to
	 * a {@link StepExecution}. This would be needed in cases where step should be executed
	 * in a specific host or rack considering data locality.
	 * <p>
	 * Default implementation returns an empty map.
	 *
	 * @param stepExecutions Set of step executions
	 * @return Mapping between step executions and container request data
	 * @throws Exception If error occurred
	 */
	protected Map<StepExecution, ContainerRequestHint> createRequestData(Set<StepExecution> stepExecutions)
			throws Exception {
		return new HashMap<StepExecution, ContainerRequestHint>();
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

		batchAppmaster.addAppmasterStateListener(new AppmasterStateListener() {
			@Override
			public void state(AppmasterState state) {
				log.info("AppmasterStateListener state=[" + state + "]");
				if(state == AppmasterState.COMPLETED || state == AppmasterState.FAILED) {
					latch.countDown();
				}
			}
		});

		batchAppmaster.addPartitionedStepExecutionStateListener(new PartitionedStepExecutionStateListener() {
			@Override
			public void state(PartitionedStepExecutionState state, StepExecution stepExecution) {
				log.info("PartitionedStepExecutionStateListener state=[" + state + "] stepExecution=[" + stepExecution
						+ "] masterStepExecution=[" + masterStepExecution + "]");
				if(state == PartitionedStepExecutionState.COMPLETED && masterStepExecution.equals(stepExecution)) {
					log.info("Got complete state for stepExecution=[" + stepExecution + "]");
					latch.countDown();
				}
			}
		});

		try {
			latch.await();
		} catch (Exception e) {
			log.warn("Latch wait interrupted, we may not be finished!");
		}
		log.info("Waiting latch complete");
	}

}
