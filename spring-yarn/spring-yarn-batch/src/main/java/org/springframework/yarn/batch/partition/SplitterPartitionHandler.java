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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.StepExecutionSplitter;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.container.ContainerRequestHint;
import org.springframework.yarn.batch.am.AbstractBatchAppmaster;

/**
 * Implementation of Spring Batch {@link PartitionHandler} which does
 * partitioning based on number of input files from HDFS.
 *
 * @author Janne Valkealahti
 *
 */
public class SplitterPartitionHandler extends AbstractPartitionHandler {

	/**
	 * Instantiates a new splitter partition handler.
	 */
	public SplitterPartitionHandler() {
		super();
	}

	/**
	 * Instantiates a new splitter partition handler.
	 *
	 * @param batchAppmaster the batch appmaster
	 */
	public SplitterPartitionHandler(AbstractBatchAppmaster batchAppmaster) {
		super(batchAppmaster);
	}

	@Override
	protected Set<StepExecution> createSplits(StepExecutionSplitter stepSplitter, StepExecution stepExecution)
			throws Exception {
		return stepSplitter.split(stepExecution, 0);
	}

	@Override
	protected Map<StepExecution, ContainerRequestHint> createRequestData(Set<StepExecution> stepExecutions) throws Exception {
		Map<StepExecution, ContainerRequestHint> requests = new HashMap<StepExecution, ContainerRequestHint>();
		for (StepExecution execution : stepExecutions) {
			String locations = execution.getExecutionContext().getString(getKeySplitLocations());
			String[] hosts = StringUtils.delimitedListToStringArray(locations, ",");
			String[] racks = new String[0];
			requests.put(execution, new ContainerRequestHint(execution, null, hosts, racks, null));
		}
		return requests;
	}

}
