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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.yarn.batch.repository.bindings.JobExecutionType;
import org.springframework.yarn.batch.repository.bindings.JobInstanceType;
import org.springframework.yarn.batch.repository.bindings.exp.FindJobInstancesByJobNameReq;
import org.springframework.yarn.batch.repository.bindings.exp.FindJobInstancesByJobNameRes;
import org.springframework.yarn.batch.repository.bindings.exp.FindRunningJobExecutionsReq;
import org.springframework.yarn.batch.repository.bindings.exp.FindRunningJobExecutionsRes;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobExecutionsReq;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobExecutionsRes;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobInstanceCountReq;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobInstanceCountRes;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobInstanceReq;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobInstanceRes;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobInstancesReq;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobInstancesRes;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobNamesReq;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobNamesRes;
import org.springframework.yarn.batch.repository.bindings.exp.GetStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.exp.GetStepExecutionRes;
import org.springframework.yarn.integration.ip.mind.AppmasterMindScOperations;

public class RemoteJobExplorer extends AbstractRemoteDao implements JobExplorer {

	public RemoteJobExplorer() {
		super();
	}

	public RemoteJobExplorer(AppmasterMindScOperations appmasterScOperations) {
		super(appmasterScOperations);
	}

	@Override
	public List<JobInstance> getJobInstances(String jobName, int start, int count) {

		List<JobInstance> jobInstances = new ArrayList<JobInstance>();
		try {
			GetJobInstancesReq request = JobRepositoryRpcFactory.buildGetJobInstancesReq(jobName, start, count);
			GetJobInstancesRes response = (GetJobInstancesRes) getAppmasterScOperations().doMindRequest(request);
			for (JobInstanceType j : response.jobInstances) {
				jobInstances.add(JobRepositoryRpcFactory.convertJobInstanceType(j));
			}
		} catch (Exception e) {
			throw convertException(e);
		}
		return jobInstances;
	}

	@Override
	public JobExecution getJobExecution(Long executionId) {
		GetJobExecutionReq request = JobRepositoryRpcFactory.buildGetJobExecutionReq(executionId);
		GetJobExecutionRes response = (GetJobExecutionRes) getAppmasterScOperations().doMindRequest(request);
		return JobRepositoryRpcFactory.convertJobExecutionType(response.jobExecution);
	}

	@Override
	public StepExecution getStepExecution(Long jobExecutionId, Long stepExecutionId) {
		GetStepExecutionReq request = JobRepositoryRpcFactory.buildGetStepExecutionReq(jobExecutionId, stepExecutionId);
		GetStepExecutionRes response = (GetStepExecutionRes) getAppmasterScOperations().doMindRequest(request);
		return JobRepositoryRpcFactory.convertStepExecutionType(response.stepExecution);
	}

	@Override
	public JobInstance getJobInstance(Long instanceId) {
		JobInstance jobInstance = null;
		try {
			GetJobInstanceReq request = JobRepositoryRpcFactory.buildGetJobInstanceReq(instanceId);
			GetJobInstanceRes response = (GetJobInstanceRes) getAppmasterScOperations().doMindRequest(request);
			jobInstance = JobRepositoryRpcFactory.convertJobInstanceType(response.jobInstance);
		} catch (Exception e) {
			throw convertException(e);
		}
		return jobInstance;
	}

	@Override
	public List<JobExecution> getJobExecutions(JobInstance jobInstance) {
		List<JobExecution> jobExecutions = new ArrayList<JobExecution>();
		GetJobExecutionsReq request = JobRepositoryRpcFactory.buildGetJobExecutionsReq(jobInstance);
		GetJobExecutionsRes response = (GetJobExecutionsRes) getAppmasterScOperations().doMindRequest(request);
		for (JobExecutionType j : response.jobExecutions) {
			jobExecutions.add(JobRepositoryRpcFactory.convertJobExecutionType(j));
		}
		return jobExecutions;
	}

	@Override
	public Set<JobExecution> findRunningJobExecutions(String jobName) {
		FindRunningJobExecutionsReq request = JobRepositoryRpcFactory.buildFindRunningJobExecutionsReq(jobName);
		Set<JobExecution> jobExecutions = new HashSet<JobExecution>();
		FindRunningJobExecutionsRes response = (FindRunningJobExecutionsRes) getAppmasterScOperations().doMindRequest(request);
		for (JobExecutionType j : response.jobExecutions) {
			jobExecutions.add(JobRepositoryRpcFactory.convertJobExecutionType(j));
		}
		return jobExecutions;
	}

	@Override
	public List<String> getJobNames() {
		GetJobNamesReq request = JobRepositoryRpcFactory.buildGetJobNamesReq();
		GetJobNamesRes response = (GetJobNamesRes) getAppmasterScOperations().doMindRequest(request);
		return response.jobNames;
	}

	@Override
	public int getJobInstanceCount(String jobName) throws NoSuchJobException {
		GetJobInstanceCountReq request = JobRepositoryRpcFactory.buildGetJobInstanceCountReq(jobName);
		GetJobInstanceCountRes response = (GetJobInstanceCountRes) getAppmasterScOperations().doMindRequest(request);
		if (response.count != null) {
			return response.count;
		}
		return 0;
	}

	@Override
	public List<JobInstance> findJobInstancesByJobName(String jobName, int start, int count) {
		List<JobInstance> jobInstances = new ArrayList<JobInstance>();
		try {
			FindJobInstancesByJobNameReq request = JobRepositoryRpcFactory.buildFindJobInstancesByJobNameReq(jobName, start, count);
			FindJobInstancesByJobNameRes response = (FindJobInstancesByJobNameRes) getAppmasterScOperations().doMindRequest(request);
			for (JobInstanceType j : response.jobInstances) {
				jobInstances.add(JobRepositoryRpcFactory.convertJobInstanceType(j));
			}
		} catch (Exception e) {
			throw convertException(e);
		}
		return jobInstances;
	}

}
