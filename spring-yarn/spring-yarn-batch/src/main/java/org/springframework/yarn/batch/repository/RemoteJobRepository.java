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
package org.springframework.yarn.batch.repository;

import java.util.Collection;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.util.Assert;
//import org.springframework.yarn.batch.repository.bindings.UpdateExecutionContextReq;
//import org.springframework.yarn.batch.repository.bindings.UpdateExecutionContextRes;
import org.springframework.yarn.batch.repository.bindings.repo.AddWithStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.repo.AddWithStepExecutionRes;
import org.springframework.yarn.batch.repository.bindings.repo.CreateJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.repo.CreateJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.repo.CreateJobExecutionWithJobInstanceReq;
import org.springframework.yarn.batch.repository.bindings.repo.CreateJobExecutionWithJobInstanceRes;
import org.springframework.yarn.batch.repository.bindings.repo.CreateJobInstanceReq;
import org.springframework.yarn.batch.repository.bindings.repo.CreateJobInstanceRes;
import org.springframework.yarn.batch.repository.bindings.repo.GetLastJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.repo.GetLastJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.repo.GetLastStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.repo.GetLastStepExecutionRes;
import org.springframework.yarn.batch.repository.bindings.repo.GetStepExecutionCountReq;
import org.springframework.yarn.batch.repository.bindings.repo.GetStepExecutionCountRes;
import org.springframework.yarn.batch.repository.bindings.repo.IsJobInstanceExistsReq;
import org.springframework.yarn.batch.repository.bindings.repo.IsJobInstanceExistsRes;
import org.springframework.yarn.batch.repository.bindings.repo.UpdateExecutionContextReq;
import org.springframework.yarn.batch.repository.bindings.repo.UpdateWithJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.repo.UpdateWithJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.repo.UpdateWithStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.repo.UpdateWithStepExecutionRes;
import org.springframework.yarn.integration.ip.mind.AppmasterMindScOperations;

public class RemoteJobRepository extends AbstractRemoteDao implements JobRepository {

	public RemoteJobRepository() {
		super();
	}

	public RemoteJobRepository(AppmasterMindScOperations appmasterScOperations) {
		super(appmasterScOperations);
	}

	@Override
	public boolean isJobInstanceExists(String jobName, JobParameters jobParameters) {
		Assert.notNull(jobName, "Job name must not be null.");
		Assert.notNull(jobParameters, "JobParameters must not be null.");

		try {
			IsJobInstanceExistsReq request = JobRepositoryRpcFactory.buildIsJobInstanceExistsReq(jobName, jobParameters);
			IsJobInstanceExistsRes response = (IsJobInstanceExistsRes) getAppmasterScOperations().doMindRequest(request);
			return response.response;
		} catch (Exception e) {
			throw convertException(e);
		}
	}

	@Override
	public JobExecution createJobExecution(String jobName, JobParameters jobParameters)
			throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

		Assert.notNull(jobName, "Job name must not be null.");
		Assert.notNull(jobParameters, "JobParameters must not be null.");

		JobExecution jobExecution = null;
		try {
			CreateJobExecutionReq request = JobRepositoryRpcFactory.buildCreateJobExecutionReq(jobName, jobParameters);
			CreateJobExecutionRes response = (CreateJobExecutionRes) getAppmasterScOperations().doMindRequest(request);
			jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(response.jobExecution);
		} catch (Exception e) {
			throw convertException(e);
		}
		return jobExecution;

	}

	@Override
	public void update(JobExecution jobExecution) {

		try {
			UpdateWithJobExecutionReq request = JobRepositoryRpcFactory.buildSaveJobExecutionReq(jobExecution);
			UpdateWithJobExecutionRes response = (UpdateWithJobExecutionRes) getAppmasterScOperations().doMindRequest(request);
			jobExecution.setId(response.getId());
			jobExecution.setVersion(response.getVersion());
		} catch (Exception e) {
			throw convertException(e);
		}

	}

	@Override
	public void add(StepExecution stepExecution) {

		try {
			AddWithStepExecutionReq request = JobRepositoryRpcFactory.buildAddWithStepExecutionReq(stepExecution);
			AddWithStepExecutionRes response = (AddWithStepExecutionRes) getAppmasterScOperations().doMindRequest(request);
			stepExecution.setId(response.getId());
			stepExecution.setVersion(response.getVersion());
		} catch (Exception e) {
			throw convertException(e);
		}
	}

	@Override
	public void addAll(Collection<StepExecution> stepExecutions) {
//		jobRepository.addAll(stepExecutions);
	}

	@Override
	public void update(StepExecution stepExecution) {
		try {
			UpdateWithStepExecutionReq request = JobRepositoryRpcFactory.buildUpdateWithStepExecutionReq(stepExecution);
			UpdateWithStepExecutionRes response = (UpdateWithStepExecutionRes) getAppmasterScOperations().doMindRequest(request);
			stepExecution.setId(response.getId());
			stepExecution.setVersion(response.getVersion());
		} catch (Exception e) {
			throw convertException(e);
		}
	}

	@Override
	public void updateExecutionContext(StepExecution stepExecution) {
		UpdateExecutionContextReq request = JobRepositoryRpcFactory.buildUpdateExecutionContextReq(stepExecution);
		getAppmasterScOperations().doMindRequest(request);
		// TODO: handle response
		//UpdateExecutionContextRes response = (UpdateExecutionContextRes) getAppmasterScOperations().doMindRequest(request);
	}

	@Override
	public void updateExecutionContext(JobExecution jobExecution) {
		UpdateExecutionContextReq request = JobRepositoryRpcFactory.buildUpdateExecutionContextReq(jobExecution);
		getAppmasterScOperations().doMindRequest(request);
		// TODO: handle response
		//UpdateExecutionContextRes response = (UpdateExecutionContextRes) getAppmasterScOperations().doMindRequest(request);
	}

	@Override
	public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {

		GetLastStepExecutionReq request = JobRepositoryRpcFactory.buildGetLastStepExecutionReq(jobInstance, stepName);
		GetLastStepExecutionRes response = (GetLastStepExecutionRes) getAppmasterScOperations().doMindRequest(request);
		return JobRepositoryRpcFactory.convertStepExecutionType(response.stepExecution);
	}

	@Override
	public int getStepExecutionCount(JobInstance jobInstance, String stepName) {
		GetStepExecutionCountReq request = JobRepositoryRpcFactory.buildGetStepExecutionCountReq(jobInstance, stepName);
		GetStepExecutionCountRes response = (GetStepExecutionCountRes) getAppmasterScOperations().doMindRequest(request);
		return response.count;
	}

	@Override
	public JobExecution getLastJobExecution(String jobName, JobParameters jobParameters) {
		GetLastJobExecutionReq request = JobRepositoryRpcFactory.buildGetLastJobExecutionReq(jobName, jobParameters);
		GetLastJobExecutionRes response = (GetLastJobExecutionRes) getAppmasterScOperations().doMindRequest(request);
		return JobRepositoryRpcFactory.convertJobExecutionType(response.jobExecution);
	}

	@Override
	public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
		Assert.notNull(jobName, "Job name must not be null.");
		Assert.notNull(jobParameters, "JobParameters must not be null.");

		JobInstance jobInstance = null;
		try {
			CreateJobInstanceReq request = JobRepositoryRpcFactory.buildCreateJobInstanceReq(jobName, jobParameters);
			CreateJobInstanceRes response = (CreateJobInstanceRes) getAppmasterScOperations().doMindRequest(request);
			jobInstance = JobRepositoryRpcFactory.convertJobInstanceType(response.jobInstance);
		} catch (Exception e) {
			throw convertException(e);
		}
		return jobInstance;
	}

	@Override
	public JobExecution createJobExecution(JobInstance jobInstance, JobParameters jobParameters, String jobConfigurationLocation) {
		Assert.notNull(jobInstance, "Job instance must not be null.");
		Assert.notNull(jobParameters, "JobParameters must not be null.");

		JobExecution jobExecution = null;
		try {
			CreateJobExecutionWithJobInstanceReq request = JobRepositoryRpcFactory.buildCreateJobExecutionWithJobInstanceReq(jobInstance, jobParameters, jobConfigurationLocation);
			CreateJobExecutionWithJobInstanceRes response = (CreateJobExecutionWithJobInstanceRes) getAppmasterScOperations().doMindRequest(request);
			jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(response.jobExecution);
		} catch (Exception e) {
			throw convertException(e);
		}
		return jobExecution;
	}

}
