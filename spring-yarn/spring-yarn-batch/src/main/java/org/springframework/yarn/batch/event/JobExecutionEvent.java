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
package org.springframework.yarn.batch.event;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.yarn.event.AbstractYarnEvent;

/**
 * Generic event representing that Batch {@link JobExecution}
 * has happened.
 *
 * @author Janne Valkealahti
 *
 */
public class JobExecutionEvent extends AbstractYarnEvent {

	private static final long serialVersionUID = 8284506301229285556L;

	private JobExecution jobExecution;

	/**
	 * Constructs event with the given {@link StepExecution}.
	 *
	 * @param source the component that published the event (never {@code null})
	 * @param jobExecution the Job execution
	 */
	public JobExecutionEvent(Object source, JobExecution jobExecution) {
		super(source);
		this.jobExecution = jobExecution;
	}

	/**
	 * Gets the job execution for this event.
	 *
	 * @return the job execution
	 */
	public JobExecution getJobExecution() {
		return jobExecution;
	}

	@Override
	public String toString() {
		return "JobExecutionEvent [jobExecution="
				+ jobExecution + "]";
	}

}
