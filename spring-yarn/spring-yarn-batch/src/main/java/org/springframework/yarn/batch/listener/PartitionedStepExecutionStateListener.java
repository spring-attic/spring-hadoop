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
package org.springframework.yarn.batch.listener;

import org.springframework.batch.core.StepExecution;
import org.springframework.yarn.listener.AppmasterStateListener.AppmasterState;

/**
 * Interface used for partitioned step to notify its state.
 *
 * @author Janne Valkealahti
 *
 */
public interface PartitionedStepExecutionStateListener {

	/**
	 * Invoked when partitioned step state is changing.
	 *
	 * @param state the {@link AppmasterState}
	 * @param stepExecution the step execution
	 */
	void state(PartitionedStepExecutionState state, StepExecution stepExecution);

	/**
	 * Enum for partitioned step states
	 */
	public enum PartitionedStepExecutionState {

		/** Appmaster completed state*/
		COMPLETED
	}

}
