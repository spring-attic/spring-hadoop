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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.util.Assert;
import org.springframework.yarn.am.RpcMessage;
import org.springframework.yarn.batch.repository.bindings.FindJobExecutionsReq;
import org.springframework.yarn.batch.repository.bindings.FindJobExecutionsRes;
import org.springframework.yarn.batch.repository.bindings.FindRunningJobExecutionsReq;
import org.springframework.yarn.batch.repository.bindings.FindRunningJobExecutionsRes;
import org.springframework.yarn.batch.repository.bindings.GetJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.GetJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.GetLastJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.GetLastJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.JobExecutionType;
import org.springframework.yarn.batch.repository.bindings.SaveJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.SaveJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.SynchronizeStatusReq;
import org.springframework.yarn.batch.repository.bindings.SynchronizeStatusRes;
import org.springframework.yarn.batch.repository.bindings.UpdateJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.UpdateJobExecutionRes;
import org.springframework.yarn.integration.ip.mind.AppmasterMindScOperations;

/**
 * Proxy implementation of {@link JobExecutionDao}. Passes dao
 * methods to a remote repository via service calls using
 * {@link RpcMessage} messages.
 *
 * @author Janne Valkealahti
 *
 */
public class RemoteJobExecutionDao extends AbstractRemoteDao implements JobExecutionDao {

	public RemoteJobExecutionDao() {
		super();
	}

	public RemoteJobExecutionDao(AppmasterMindScOperations appmasterScOperations) {
		super(appmasterScOperations);
	}

	@Override
	public void saveJobExecution(JobExecution jobExecution) {
		validateJobExecution(jobExecution);
		try {
			SaveJobExecutionReq request = JobRepositoryRpcFactory.buildSaveJobExecutionReq(jobExecution);
			SaveJobExecutionRes response = (SaveJobExecutionRes) getAppmasterScOperations().doMindRequest(request);
			jobExecution.setId(response.getId());
			jobExecution.setVersion(response.getVersion());
		} catch (Exception e) {
			throw convertException(e);
		}
	}

	@Override
	public void updateJobExecution(JobExecution jobExecution) {
		validateJobExecution(jobExecution);
		Assert.notNull(jobExecution.getId(),
				"JobExecution ID cannot be null. JobExecution must be saved before it can be updated");
		Assert.notNull(jobExecution.getVersion(),
				"JobExecution version cannot be null. JobExecution must be saved before it can be updated");

		try {
			UpdateJobExecutionReq request = JobRepositoryRpcFactory.buildUpdateJobExecutionReq(jobExecution);
			UpdateJobExecutionRes response = (UpdateJobExecutionRes) getAppmasterScOperations().doMindRequest(request);
			checkResponseMayThrow(response);
			jobExecution.setVersion(response.getVersion());
		} catch (Exception e) {
			throw convertException(e);
		}
	}

	@Override
	public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
		List<JobExecution> jobExecutions = new ArrayList<JobExecution>();
		try {
			FindJobExecutionsReq request = JobRepositoryRpcFactory.buildFindJobExecutionsReq(jobInstance);
			FindJobExecutionsRes response = (FindJobExecutionsRes) getAppmasterScOperations().doMindRequest(request);
			for(JobExecutionType jobExecutionType : response.jobExecutions) {
				jobExecutions.add(JobRepositoryRpcFactory.convertJobExecutionType(jobExecutionType));
			}
		} catch (Exception e) {
			throw convertException(e);
		}
		return jobExecutions;
	}

	@Override
	public JobExecution getLastJobExecution(JobInstance jobInstance) {
		JobExecution jobExecution = null;
		try {
			GetLastJobExecutionReq request = JobRepositoryRpcFactory.buildGetLastJobExecutionReq(jobInstance);
			GetLastJobExecutionRes response = (GetLastJobExecutionRes) getAppmasterScOperations().doMindRequest(request);
			if(response.jobExecution != null) {
				jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(response.jobExecution);
			}
		} catch (Exception e) {
			throw convertException(e);
		}
		return jobExecution;
	}

	@Override
	public Set<JobExecution> findRunningJobExecutions(String jobName) {
		Set<JobExecution> jobExecutions = new HashSet<JobExecution>();
		try {
			FindRunningJobExecutionsReq request = JobRepositoryRpcFactory.buildFindRunningJobExecutionsReq(jobName);
			FindRunningJobExecutionsRes response = (FindRunningJobExecutionsRes) getAppmasterScOperations().doMindRequest(request);
			for(JobExecutionType jobExecutionType : response.jobExecutions) {
				jobExecutions.add(JobRepositoryRpcFactory.convertJobExecutionType(jobExecutionType));
			}
		} catch (Exception e) {
			throw convertException(e);
		}
		return jobExecutions;
	}

	@Override
	public JobExecution getJobExecution(Long executionId) {
		JobExecution jobExecution = null;
		try {
			GetJobExecutionReq request = JobRepositoryRpcFactory.buildGetJobExecutionReq(executionId);
			GetJobExecutionRes response = (GetJobExecutionRes) getAppmasterScOperations().doMindRequest(request);
			if(response.jobExecution != null) {
				jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(response.jobExecution);
			}
		} catch (Exception e) {
			throw convertException(e);
		}
		return jobExecution;
	}

	@Override
	public void synchronizeStatus(JobExecution jobExecution) {
		try {
			SynchronizeStatusReq request = JobRepositoryRpcFactory.buildSynchronizeStatusReq(jobExecution);
			SynchronizeStatusRes response = (SynchronizeStatusRes) getAppmasterScOperations().doMindRequest(request);
			checkResponseMayThrow(response);
			if(jobExecution.getVersion().intValue() != response.getVersion().intValue()) {
				jobExecution.upgradeStatus(response.status);
				jobExecution.setVersion(response.version);
			}
		} catch (Exception e) {
			throw convertException(e);
		}
	}

	/**
	 * Validate JobExecution. At a minimum, JobId, StartTime, EndTime, and
	 * Status cannot be null.
	 *
	 * @param jobExecution
	 * @throws IllegalArgumentException
	 */
	private void validateJobExecution(JobExecution jobExecution) {
		Assert.notNull(jobExecution);
		Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
		Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
		Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
	}

}
