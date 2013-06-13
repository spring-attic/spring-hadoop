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
package org.springframework.yarn.batch.repository;

import java.util.ArrayList;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.util.Assert;
import org.springframework.yarn.am.RpcMessage;
import org.springframework.yarn.batch.repository.bindings.AddStepExecutionsReq;
import org.springframework.yarn.batch.repository.bindings.AddStepExecutionsRes;
import org.springframework.yarn.batch.repository.bindings.GetStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.GetStepExecutionRes;
import org.springframework.yarn.batch.repository.bindings.SaveStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.SaveStepExecutionRes;
import org.springframework.yarn.batch.repository.bindings.UpdateStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.UpdateStepExecutionRes;
import org.springframework.yarn.integration.ip.mind.AppmasterMindScOperations;

/**
 * Proxy implementation of {@link StepExecutionDao}. Passes dao
 * methods to a remote repository via service calls using
 * {@link RpcMessage} messages.
 *
 * @author Janne Valkealahti
 *
 */
public class RemoteStepExecutionDao extends AbstractRemoteDao implements StepExecutionDao {

	public RemoteStepExecutionDao() {
		super();
	}

	public RemoteStepExecutionDao(AppmasterMindScOperations appmasterScOperations) {
		super(appmasterScOperations);
	}

	@Override
	public void saveStepExecution(StepExecution stepExecution) {
		Assert.isNull(stepExecution.getId(), "StepExecution can't already have an id assigned");
		Assert.isNull(stepExecution.getVersion(), "StepExecution can't already have a version assigned");

		try {
			SaveStepExecutionReq request = JobRepositoryRpcFactory.buildSaveStepExecutionReq(stepExecution);
			SaveStepExecutionRes response = (SaveStepExecutionRes) getAppmasterScOperations().doMindRequest(request);
			checkResponseMayThrow(response);
			stepExecution.setId(response.getId());
			stepExecution.setVersion(response.getVersion());
		} catch (Exception e) {
			throw convertException(e);
		}
	}

	@Override
	public void updateStepExecution(StepExecution stepExecution) {
		try {
			UpdateStepExecutionReq request = JobRepositoryRpcFactory.buildUpdateStepExecutionReq(stepExecution);
			UpdateStepExecutionRes response = (UpdateStepExecutionRes) getAppmasterScOperations().doMindRequest(request);
			checkResponseMayThrow(response);
			stepExecution.setId(response.getId());
			stepExecution.setVersion(response.getVersion());
		} catch (Exception e) {
			throw convertException(e);
		}
	}

	@Override
	public StepExecution getStepExecution(JobExecution jobExecution, Long stepExecutionId) {
		StepExecution stepExecution = null;
		try {
			GetStepExecutionReq request = JobRepositoryRpcFactory.buildGetStepExecutionReq(jobExecution, stepExecutionId);
			GetStepExecutionRes response = (GetStepExecutionRes) getAppmasterScOperations().doMindRequest(request);
			if(response.stepExecution != null) {
				stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(response.stepExecution);
			}
		} catch (Exception e) {
			throw convertException(e);
		}
		return stepExecution;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void addStepExecutions(JobExecution jobExecution) {
		try {
			AddStepExecutionsReq request = JobRepositoryRpcFactory.buildAddStepExecutionsReq(jobExecution);
			AddStepExecutionsRes response = (AddStepExecutionsRes) getAppmasterScOperations().doMindRequest(request);
			checkResponseMayThrow(response);
			JobExecution retrievedJobExecution = JobRepositoryRpcFactory.convertJobExecutionType(response.jobExecution);
			jobExecution.addStepExecutions(new ArrayList(retrievedJobExecution.getStepExecutions()));
		} catch (Exception e) {
			throw convertException(e);
		}
	}

}
