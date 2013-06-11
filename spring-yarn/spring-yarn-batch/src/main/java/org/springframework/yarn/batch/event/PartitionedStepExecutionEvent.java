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
package org.springframework.yarn.batch.event;

import org.springframework.batch.core.StepExecution;
import org.springframework.yarn.event.AbstractYarnEvent;

/**
 * Generic event representing that partitioned {@link StepExecution} state
 * has been changed.
 *
 * @author Janne Valkealahti
 *
 */
@SuppressWarnings("serial")
public class PartitionedStepExecutionEvent extends AbstractYarnEvent {

	private StepExecution stepExecution;

	/**
	 * Constructs event with the given {@link StepExecution}.
	 *
	 * @param source the component that published the event (never {@code null})
	 * @param stepExecution the Step execution
	 */
	public PartitionedStepExecutionEvent(Object source, StepExecution stepExecution) {
		super(source);
		this.stepExecution = stepExecution;
	}

	/**
	 * Gets the step execution for this event.
	 *
	 * @return the step execution
	 */
	public StepExecution getStepExecution() {
		return stepExecution;
	}

	@Override
	public String toString() {
		return "PartitionedStepExecutionEvent [stepExecution="
				+ stepExecution + "]";
	}

}
