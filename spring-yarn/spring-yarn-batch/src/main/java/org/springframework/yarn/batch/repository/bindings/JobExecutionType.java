/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.yarn.batch.repository.bindings;

import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

/**
 * Binding for {@link JobExecution}.
 *
 * @author Janne Valkealahti
 *
 */
public class JobExecutionType extends BaseObject {

	public Long id;
	public Integer version;
	public String jobConfigurationLocation;
	public JobInstanceType jobInstance;
	public List<StepExecutionType> stepExecutions;
	public BatchStatus status;
	public Long startTime;
	public Long createTime;
	public Long endTime;
	public Long lastUpdated;
	public String exitStatus;
	public ExecutionContextType executionContext;
	public JobParametersType jobParameters;

	public JobExecutionType() {
		super("JobExecutionType");
	}

}
