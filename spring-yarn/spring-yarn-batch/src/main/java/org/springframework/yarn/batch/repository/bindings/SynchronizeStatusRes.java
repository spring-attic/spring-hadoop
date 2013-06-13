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
package org.springframework.yarn.batch.repository.bindings;

import org.springframework.batch.core.BatchStatus;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

/**
 * Response for synching status.
 *
 * @author Janne Valkealahti
 *
 * @see org.springframework.yarn.batch.repository.RemoteJobExecutionDao#synchronizeStatus(JobExecution)
 * @see org.springframework.batch.core.JobExecution
 *
 */
public class SynchronizeStatusRes extends BaseResponseObject {

	public Integer version;
	public BatchStatus status;

	public SynchronizeStatusRes() {
		super("SynchronizeStatusRes");
	}

	public SynchronizeStatusRes(Integer version, BatchStatus status) {
		this();
		this.version = version;
		this.status = status;
	}

	public Integer getVersion() {
		return version;
	}

	public BatchStatus getStatus() {
		return status;
	}

}
