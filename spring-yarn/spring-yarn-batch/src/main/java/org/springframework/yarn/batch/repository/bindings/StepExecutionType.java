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
package org.springframework.yarn.batch.repository.bindings;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

/**
 * Binding for {@link StepExecution}.
 *
 * @author Janne Valkealahti
 *
 */
public class StepExecutionType extends BaseObject {

	public Long id;
	public Integer version;
	public JobExecutionType jobExecution;
	public String stepName;
	public BatchStatus status;
	public Integer readCount = 0;
	public Integer writeCount = 0;
	public Integer commitCount = 0;
	public Integer rollbackCount = 0;
	public Integer readSkipCount = 0;
	public Integer processSkipCount = 0;
	public Integer writeSkipCount = 0;
	public Long startTime;
	public Long endTime;
	public Long lastUpdated;
	public ExecutionContextType executionContext;
	public String exitStatus;
	public Boolean terminateOnly;
	public Integer filterCount;

	public StepExecutionType() {
		super("StepExecutionType");
	}

}
