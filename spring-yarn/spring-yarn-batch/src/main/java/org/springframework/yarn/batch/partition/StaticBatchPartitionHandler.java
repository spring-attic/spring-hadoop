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

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.StepExecutionSplitter;
import org.springframework.yarn.batch.am.AbstractBatchAppmaster;

/**
 * Implementation of Spring Batch {@link PartitionHandler} which does
 * partitioning based on static grid size.
 *
 * @author Janne Valkealahti
 *
 */
public class StaticBatchPartitionHandler extends AbstractBatchPartitionHandler {

	private static final Log log = LogFactory.getLog(StaticBatchPartitionHandler.class);

	/** Grid size for partitioning */
	private int gridSize = 1;

	/**
	 * Instantiates a new static batch partition handler.
	 *
	 * @param batchAppmaster the batch appmaster
	 */
	public StaticBatchPartitionHandler(AbstractBatchAppmaster batchAppmaster) {
		super(batchAppmaster);
	}

	/**
	 * Instantiates a new static batch partition handler.
	 *
	 * @param batchAppmaster the batch appmaster
	 * @param gridSize the grid size
	 */
	public StaticBatchPartitionHandler(AbstractBatchAppmaster batchAppmaster, int gridSize) {
		super(batchAppmaster);
		this.gridSize = gridSize;
	}

	/**
	 * Passed to the {@link StepExecutionSplitter} in the
	 * {@link #handle(StepExecutionSplitter, StepExecution)} method, instructing
	 * it how many {@link StepExecution} instances are required, ideally. The
	 * {@link StepExecutionSplitter} is allowed to ignore the grid size in the
	 * case of a restart, since the input data partitions must be preserved.
	 *
	 * @param gridSize the number of step executions that will be created
	 */
	public void setGridSize(int gridSize) {
		this.gridSize = gridSize;
	}

	@Override
	protected Set<StepExecution> createStepExecutionSplits(StepExecutionSplitter stepSplitter, StepExecution stepExecution)
			throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("Creating splits for stepExecution=" + stepExecution + " with gridSize=" + gridSize);
		}
		return stepSplitter.split(stepExecution, gridSize);
	}

}
