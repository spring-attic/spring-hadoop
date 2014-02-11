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
package org.springframework.yarn.batch.am;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.springframework.batch.core.StepExecution;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.container.ContainerRequestHint;
import org.springframework.yarn.batch.listener.PartitionedStepExecutionStateListener;

/**
 * Extended interface {@link YarnAppmaster}s used with Spring Batch.
 *
 * @author Janne Valkealahti
 *
 */
public interface BatchYarnAppmaster extends YarnAppmaster {

	/**
	 * Adds the step splits.
	 *
	 * @param stepExecution the step execution
	 * @param stepName the step name
	 * @param split the step execution splits
	 * @param resourceRequests the resource requests
	 */
	void addStepSplits(StepExecution stepExecution, String stepName, Set<StepExecution> split,
			Map<StepExecution, ContainerRequestHint> resourceRequests);

	/**
	 * Gets the step executions.
	 *
	 * @return the step executions
	 */
	Collection<? extends StepExecution> getStepExecutions();

	/**
	 * Adds the partitioned step execution state listener.
	 *
	 * @param partitionedStepExecutionStateListener the partitioned step execution state listener
	 */
	void addPartitionedStepExecutionStateListener(
			PartitionedStepExecutionStateListener partitionedStepExecutionStateListener);

}
