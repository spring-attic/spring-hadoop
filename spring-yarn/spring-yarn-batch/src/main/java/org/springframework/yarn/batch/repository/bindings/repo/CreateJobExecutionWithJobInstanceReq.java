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
package org.springframework.yarn.batch.repository.bindings.repo;

import java.util.Map;

import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.yarn.batch.repository.bindings.JobInstanceType;
import org.springframework.yarn.batch.repository.bindings.JobParameterType;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

/**
 * Request binding for {@link JobRepository#createJobExecution(JobInstance, JobParameters, String)}.
 *
 * @author Janne Valkealahti
 *
 */
public class CreateJobExecutionWithJobInstanceReq extends BaseObject {

	public JobInstanceType jobInstance;
	public Map<String, JobParameterType> jobParameters;
	public String jobConfigurationLocation;

	public CreateJobExecutionWithJobInstanceReq() {
		super("CreateJobExecutionWithJobInstanceReq");
	}

}
