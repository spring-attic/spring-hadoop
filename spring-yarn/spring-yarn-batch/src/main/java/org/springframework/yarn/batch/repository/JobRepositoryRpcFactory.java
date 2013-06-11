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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameter.ParameterType;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.yarn.batch.repository.bindings.AddStepExecutionsReq;
import org.springframework.yarn.batch.repository.bindings.CreateJobInstanceReq;
import org.springframework.yarn.batch.repository.bindings.ExecutionContextType;
import org.springframework.yarn.batch.repository.bindings.ExecutionContextType.ObjectEntry;
import org.springframework.yarn.batch.repository.bindings.FindJobExecutionsReq;
import org.springframework.yarn.batch.repository.bindings.FindRunningJobExecutionsReq;
import org.springframework.yarn.batch.repository.bindings.GetExecutionContextReq;
import org.springframework.yarn.batch.repository.bindings.GetJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.GetJobInstanceByIdReq;
import org.springframework.yarn.batch.repository.bindings.GetJobInstanceReq;
import org.springframework.yarn.batch.repository.bindings.GetJobInstancesReq;
import org.springframework.yarn.batch.repository.bindings.GetJobNamesReq;
import org.springframework.yarn.batch.repository.bindings.GetLastJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.GetStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.JobExecutionType;
import org.springframework.yarn.batch.repository.bindings.JobInstanceType;
import org.springframework.yarn.batch.repository.bindings.JobParameterType;
import org.springframework.yarn.batch.repository.bindings.JobParametersType;
import org.springframework.yarn.batch.repository.bindings.SaveExecutionContextReq;
import org.springframework.yarn.batch.repository.bindings.SaveJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.SaveStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.StepExecutionType;
import org.springframework.yarn.batch.repository.bindings.SynchronizeStatusReq;
import org.springframework.yarn.batch.repository.bindings.UpdateExecutionContextReq;
import org.springframework.yarn.batch.repository.bindings.UpdateJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.UpdateStepExecutionReq;

/**
 * Helper class providing factory methods for building requests used
 * in remote job repository functionality.
 *
 * @author Janne Valkealahti
 *
 */
public class JobRepositoryRpcFactory {

	/**
	 * Builds request for saving a step execution.
	 *
	 * @param stepExecution the step execution
	 * @return the {@link SaveStepExecutionReq} request
	 */
	public static SaveStepExecutionReq buildSaveStepExecutionReq(StepExecution stepExecution) {
		SaveStepExecutionReq req = new SaveStepExecutionReq();
		req.stepExecution = convertStepExecutionType(stepExecution);
		return req;
	}

	/**
	 * Builds request for adding step executionf from a job execution.
	 *
	 * @param jobExecution the job execution
	 * @return the {@link AddStepExecutionsReq} request
	 */
	public static AddStepExecutionsReq buildAddStepExecutionsReq(JobExecution jobExecution) {
		AddStepExecutionsReq req = new AddStepExecutionsReq();
		req.jobExecution = convertJobExecutionType(jobExecution);
		return req;
	}

	/**
	 * Builds request for updating a step execution.
	 *
	 * @param stepExecution the step execution
	 * @return the {@link UpdateStepExecutionReq} request
	 */
	public static UpdateStepExecutionReq buildUpdateStepExecutionReq(StepExecution stepExecution) {
		UpdateStepExecutionReq req = new UpdateStepExecutionReq();
		req.stepExecution = convertStepExecutionType(stepExecution);
		return req;
	}

	/**
	 * Builds request for getting a step execution job execution
	 * and step execution id.
	 *
	 * @param jobExecution the job execution
	 * @param stepExecutionId the step execution id
	 * @return the {@link GetStepExecutionReq} request
	 */
	public static GetStepExecutionReq buildGetStepExecutionReq(JobExecution jobExecution, Long stepExecutionId) {
		GetStepExecutionReq req = new GetStepExecutionReq();
		req.jobExecution = convertJobExecutionType(jobExecution);
		req.stepExecutionId = stepExecutionId;
		return req;
	}

	/**
	 * Builds request for updating a job execution.
	 *
	 * @param jobExecution the job execution
	 * @return the {@link UpdateJobExecutionReq} request
	 */
	public static UpdateJobExecutionReq buildUpdateJobExecutionReq(JobExecution jobExecution) {
		UpdateJobExecutionReq req = new UpdateJobExecutionReq();
		req.jobExecution = convertJobExecutionType(jobExecution);
		return req;
	}

	/**
	 * Builds request for synchronizing a job execution status.
	 *
	 * @param jobExecution the job execution
	 * @return the {@link SynchronizeStatusReq} request
	 */
	public static SynchronizeStatusReq buildSynchronizeStatusReq(JobExecution jobExecution) {
		SynchronizeStatusReq req = new SynchronizeStatusReq();
		req.jobExecution = convertJobExecutionType(jobExecution);
		return req;
	}

	/**
	 * Builds request for saving a job execution.
	 *
	 * @param jobExecution the job execution
	 * @return the {@link SaveJobExecutionReq} request
	 */
	public static SaveJobExecutionReq buildSaveJobExecutionReq(JobExecution jobExecution) {
		SaveJobExecutionReq req = new SaveJobExecutionReq();
		req.jobExecution = convertJobExecutionType(jobExecution);
		return req;
	}

	/**
	 * Builds request for saving execution context from a step execution.
	 *
	 * @param stepExecution the step execution
	 * @return the {@link SaveExecutionContextReq} request
	 */
	public static SaveExecutionContextReq buildSaveExecutionContextReq(StepExecution stepExecution) {
		SaveExecutionContextReq req = new SaveExecutionContextReq();
		req.stepExecution = convertStepExecutionType(stepExecution);
		return req;
	}

	/**
	 * Builds request for saving execution context from a job execution.
	 *
	 * @param jobExecution the job execution
	 * @return the {@link SaveExecutionContextReq} request
	 */
	public static SaveExecutionContextReq buildSaveExecutionContextReq(JobExecution jobExecution) {
		SaveExecutionContextReq req = new SaveExecutionContextReq();
		req.jobExecution = convertJobExecutionType(jobExecution);
		return req;
	}

	/**
	 * Builds request for updating execution context from a step execution.
	 *
	 * @param stepExecution the step execution
	 * @return the {@link UpdateExecutionContextReq} request
	 */
	public static UpdateExecutionContextReq buildUpdateExecutionContextReq(StepExecution stepExecution) {
		UpdateExecutionContextReq req = new UpdateExecutionContextReq();
		req.stepExecution = convertStepExecutionType(stepExecution);
		return req;
	}

	/**
	 * Builds request for updating execution context from a job execution.
	 *
	 * @param jobExecution the job execution
	 * @return the {@link UpdateExecutionContextReq} request
	 */
	public static UpdateExecutionContextReq buildUpdateExecutionContextReq(JobExecution jobExecution) {
		UpdateExecutionContextReq req = new UpdateExecutionContextReq();
		req.jobExecution = convertJobExecutionType(jobExecution);
		return req;
	}

	/**
	 * Builds request for getting execution context from a step execution.
	 *
	 * @param stepExecution the step execution
	 * @return the {@link GetExecutionContextReq} request
	 */
	public static GetExecutionContextReq buildGetExecutionContextReq(StepExecution stepExecution) {
		GetExecutionContextReq req = new GetExecutionContextReq();
		req.stepExecution = convertStepExecutionType(stepExecution);
		return req;
	}

	/**
	 * Builds request for getting execution context from a job execution.
	 *
	 * @param jobExecution the step execution
	 * @return the {@link GetExecutionContextReq} request
	 */
	public static GetExecutionContextReq buildGetExecutionContextReq(JobExecution jobExecution) {
		GetExecutionContextReq req = new GetExecutionContextReq();
		req.jobExecution = convertJobExecutionType(jobExecution);
		return req;
	}

	/**
	 * Creates {@link StepExecutionType} from {@link StepExecution}.
	 *
	 * @param stepExecution the step execution
	 * @return the step execution type
	 * @see #convertStepExecutionType(StepExecution, JobExecution)
	 */
	public static StepExecutionType convertStepExecutionType(StepExecution stepExecution) {
		return convertStepExecutionType(stepExecution, null);
	}

	/**
	 * Creates {@link StepExecutionType} from {@link StepExecution}. Second
	 * argument {@link JobExecution} is only used as a back reference to
	 * prevent never ending loop for serialization logic due to references
	 * between {@link StepExecution} and {@link JobExecution}.
	 *
	 * @param stepExecution the step execution
	 * @param jobExecution the job execution
	 * @return the step execution type
	 */
	public static StepExecutionType convertStepExecutionType(StepExecution stepExecution, JobExecution jobExecution) {
		StepExecutionType type = new StepExecutionType();
		type.id = stepExecution.getId();
		type.version = stepExecution.getVersion();
		type.stepName = stepExecution.getStepName();

		type.jobExecution = convertJobExecutionType(stepExecution.getJobExecution(), stepExecution);

		type.status = stepExecution.getStatus();
		type.readCount = stepExecution.getReadCount();
		type.writeCount = stepExecution.getWriteCount();
		type.commitCount = stepExecution.getCommitCount();
		type.rollbackCount = stepExecution.getRollbackCount();
		type.readSkipCount = stepExecution.getReadSkipCount();
		type.processSkipCount = stepExecution.getProcessSkipCount();
		type.writeSkipCount = stepExecution.getWriteSkipCount();
		type.startTime = nullsafeToMillis(stepExecution.getStartTime());
		type.endTime = nullsafeToMillis(stepExecution.getEndTime());
		type.lastUpdated = nullsafeToMillis(stepExecution.getLastUpdated());
		type.terminateOnly = stepExecution.isTerminateOnly();
		type.filterCount = stepExecution.getFilterCount();
		type.executionContext = convertExecutionContext(stepExecution.getExecutionContext());

		return type;
	}

	/**
	 * Converts a {@link ExecutionContext} to {@link ExecutionContextType}.
	 *
	 * @param executionContext the execution context
	 * @return converted execution context type
	 */
	public static ExecutionContextType convertExecutionContext(ExecutionContext executionContext) {
		ExecutionContextType type = new ExecutionContextType();
		type.map = new HashMap<String, ExecutionContextType.ObjectEntry>();
		for(Entry<String, Object> entry : executionContext.entrySet()) {
			Object value = entry.getValue();
			type.map.put(entry.getKey(), new ExecutionContextType.ObjectEntry(value, value.getClass().getCanonicalName()));
		}
		return type;
	}

	/**
	 * Converts a {@link ExecutionContextType} to {@link ExecutionContext}.
	 *
	 * @param executionContextType the execution context type
	 * @return converted execution context
	 */
	public static ExecutionContext convertExecutionContextType(ExecutionContextType executionContextType) {
		Map<String, Object> map = new ConcurrentHashMap<String, Object>();

		for(Entry<String, ObjectEntry> entry : executionContextType.map.entrySet()) {
			String key = entry.getKey();
			ObjectEntry objectEntry = entry.getValue();
			Object value = null;
			if(String.class.getCanonicalName().equals(objectEntry.clazz)) {
				value = objectEntry.obj;
			} else if(Integer.class.getCanonicalName().equals(objectEntry.clazz)) {
				value = objectEntry.obj;
			} else if(Long.class.getCanonicalName().equals(objectEntry.clazz)) {
				if(objectEntry.obj instanceof Integer) {
					value = new Long((Integer)objectEntry.obj);
				} else {
					value = objectEntry.obj;
				}
			} else if(Double.class.getCanonicalName().equals(objectEntry.clazz)) {
				value = objectEntry.obj;
			}
			if(value != null) {
				// should we throw error if null?
				map.put(key, value);
			}
		}

		return new ExecutionContext(map);
	}

	/**
	 * Converts a {@link StepExecutionType} to {@link StepExecution}.
	 *
	 * @param type the step execution type
	 * @return converted step execution
	 */
	public static StepExecution convertStepExecutionType(StepExecutionType type) {
		JobExecution jobExecution = convertJobExecutionType(type.jobExecution);
		StepExecution stepExecution = type.id != null ? new StepExecution(type.stepName, jobExecution, type.id)
				: new StepExecution(type.stepName, jobExecution);
		stepExecution.setVersion(type.version);
		stepExecution.setStatus(type.status);
		stepExecution.setReadCount(type.readCount);
		stepExecution.setWriteCount(type.writeCount);
		stepExecution.setCommitCount(type.commitCount);
		stepExecution.setRollbackCount(type.rollbackCount);
		stepExecution.setReadSkipCount(type.readSkipCount);
		stepExecution.setProcessSkipCount(type.processSkipCount);
		stepExecution.setWriteSkipCount(type.writeSkipCount);

		stepExecution.setStartTime(nullsafeToDate(type.startTime));
		stepExecution.setEndTime(nullsafeToDate(type.endTime));
		stepExecution.setLastUpdated(nullsafeToDate(type.lastUpdated));

		if(type.terminateOnly.booleanValue()) {
			stepExecution.setTerminateOnly();
		}
		stepExecution.setFilterCount(type.filterCount);

		ExecutionContext executionContext = convertExecutionContextType(type.executionContext);
		stepExecution.setExecutionContext(executionContext);

		return stepExecution;
	}

	/**
	 * Creates {@link JobExecutionType} from {@link JobExecution}.
	 *
	 * @param jobExecution the job execution
	 * @return the job execution type
	 * @see #convertJobExecutionType(JobExecution, StepExecution)
	 */
	public static JobExecutionType convertJobExecutionType(JobExecution jobExecution) {
		return convertJobExecutionType(jobExecution, null);
	}

	/**
	 * Creates {@link JobExecutionType} from {@link JobExecution}. Second
	 * argument {@link StepExecution} is only used as a back reference to
	 * prevent never ending loop for serialization logic due to references
	 * between {@link StepExecution} and {@link JobExecution}.
	 *
	 * @param jobExecution the job execution
	 * @param stepExecution the step execution
	 * @return the job execution type
	 */
	public static JobExecutionType convertJobExecutionType(JobExecution jobExecution, StepExecution stepExecution) {
		JobExecutionType type = new JobExecutionType();
		type.id = jobExecution.getId();
		type.version = jobExecution.getVersion();

		type.jobInstance = convertJobInstanceType(jobExecution.getJobInstance());

		type.status = jobExecution.getStatus();
		type.startTime = nullsafeToMillis(jobExecution.getStartTime());
		type.endTime = nullsafeToMillis(jobExecution.getEndTime());
		type.createTime = nullsafeToMillis(jobExecution.getCreateTime());
		type.lastUpdated = nullsafeToMillis(jobExecution.getLastUpdated());
		type.exitStatus = jobExecution.getExitStatus().getExitCode();

		type.stepExecutions = new ArrayList<StepExecutionType>();

		for (StepExecution stepExecution2 : jobExecution.getStepExecutions()) {
			if(!jobExecution.getStepExecutions().contains(stepExecution2) || stepExecution == null) {
				type.stepExecutions.add(convertStepExecutionType(stepExecution2, jobExecution));
			}
		}

		type.executionContext = convertExecutionContext(jobExecution.getExecutionContext());

		return type;
	}

	/**
	 * Converts a {@link JobExecutionType} to {@link JobExecution}.
	 *
	 * @param type the job execution type
	 * @return converted job execution
	 */
	public static JobExecution convertJobExecutionType(JobExecutionType type) {
		JobInstance jobInstance = convertJobInstanceType(type.jobInstance);
		JobExecution jobExecution = new JobExecution(jobInstance, type.id);
		jobExecution.setVersion(type.version);
		jobExecution.setStatus(type.status);
		jobExecution.setStartTime(nullsafeToDate(type.startTime));
		jobExecution.setEndTime(nullsafeToDate(type.endTime));
		jobExecution.setCreateTime(nullsafeToDate(type.createTime));
		jobExecution.setLastUpdated(nullsafeToDate(type.lastUpdated));
		jobExecution.setExitStatus(new ExitStatus(type.exitStatus));

		List<StepExecution> stepExecutions = new ArrayList<StepExecution>();
		for(StepExecutionType stepExecutionType : type.stepExecutions) {
			StepExecution convertStepExecutionType = convertStepExecutionType(stepExecutionType);
			stepExecutions.add(convertStepExecutionType);
		}
		jobExecution.addStepExecutions(stepExecutions);

		ExecutionContext executionContext = convertExecutionContextType(type.executionContext);

		jobExecution.setExecutionContext(executionContext);

		return jobExecution;
	}

	/**
	 * Converts a {@link JobInstance} to {@link JobInstanceType}.
	 *
	 * @param jobInstance the job instance
	 * @return converted job instance type
	 */
	public static JobInstanceType convertJobInstanceType(JobInstance jobInstance) {
		JobInstanceType type = new JobInstanceType();
		type.id = jobInstance.getId();
		type.version = jobInstance.getVersion();
		type.jobParameters = convertJobParameters(jobInstance.getJobParameters());
		type.jobName = jobInstance.getJobName();
		return type;
	}

	/**
	 * Converts a {@link JobInstanceType} to {@link JobInstance}.
	 *
	 * @param type the job instance type
	 * @return converted job instance
	 */
	public static JobInstance convertJobInstanceType(JobInstanceType type) {
		JobParameters jobParameters = convertJobParametersType(type.jobParameters);
		JobInstance jobInstance = new JobInstance(type.id, jobParameters, type.jobName);
		jobInstance.setVersion(type.version);
		return jobInstance;
	}

	/**
	 * Converts a {@link JobParameters} to {@link JobParametersType}.
	 *
	 * @param jobParameters the job parameters
	 * @return converted job parameters type
	 */
	public static JobParametersType convertJobParameters(JobParameters jobParameters) {
		JobParametersType type = new JobParametersType();
		type.parameters = new HashMap<String, JobParameterType>();

		Map<String, JobParameter> parameters = jobParameters.getParameters();
		for(Entry<String, JobParameter> entry : parameters.entrySet()) {
			JobParameterType jobParameterType = new JobParameterType();
			jobParameterType.parameter = entry.getValue().getValue();
			jobParameterType.parameterType = entry.getValue().getType();
			type.parameters.put(entry.getKey(), jobParameterType);
		}

		return type;
	}

	/**
	 * Converts a {@link JobParametersType} to {@link JobParameters}.
	 *
	 * @param type the job parameters type
	 * @return converted job parameters
	 */
	public static JobParameters convertJobParametersType(JobParametersType type) {

		Map<String, JobParameter> map = new HashMap<String, JobParameter>();
		for(Entry<String, JobParameterType> entry : type.parameters.entrySet()) {
			ParameterType parameterType = entry.getValue().parameterType;
			if(parameterType == ParameterType.DATE) {
				if(entry.getValue().parameter instanceof Integer) {
					map.put(entry.getKey(), new JobParameter(new Date((Integer)entry.getValue().parameter)));
				} else if(entry.getValue().parameter instanceof Date) {
					map.put(entry.getKey(), new JobParameter(((Date)entry.getValue().parameter)));
				}
			} else if(parameterType == ParameterType.DOUBLE) {
				map.put(entry.getKey(), new JobParameter((Double)entry.getValue().parameter));
			} else if(parameterType == ParameterType.LONG) {
				if(entry.getValue().parameter instanceof Long) {
					map.put(entry.getKey(), new JobParameter((Long)entry.getValue().parameter));
				} else if(entry.getValue().parameter instanceof Integer) {
					Long tmp = new Long((Integer)entry.getValue().parameter);
					map.put(entry.getKey(), new JobParameter(tmp));
				}
			} else if(parameterType == ParameterType.STRING) {
				map.put(entry.getKey(), new JobParameter((String)entry.getValue().parameter));
			}
		}
		return new JobParameters(map);
	}

	/**
	 * Builds request for creating a job instance.
	 *
	 * @param jobName the job name
	 * @param jobParameters the job parameters
	 * @return the {@link CreateJobInstanceReq} request
	 */
	public static CreateJobInstanceReq buildCreateJobInstanceReq(String jobName, JobParameters jobParameters) {
		CreateJobInstanceReq req = new CreateJobInstanceReq();

		Map<String, JobParameterType> map = new HashMap<String, JobParameterType>();
		for(Entry<String, JobParameter> parameter : jobParameters.getParameters().entrySet()) {
			JobParameterType type = new JobParameterType();
			type.parameter = parameter.getValue().getValue();
			type.parameterType = parameter.getValue().getType();
			map.put(parameter.getKey(), type);
		}

		req.jobName = jobName;
		req.jobParameters = map;
		return req;
	}

	/**
	 * Builds request for getting a job instance.
	 *
	 * @param jobName the job name
	 * @param jobParameters the job parameters
	 * @return the {@link GetJobInstanceReq} request
	 */
	public static GetJobInstanceReq buildGetJobInstanceReq(String jobName, JobParameters jobParameters) {
		GetJobInstanceReq req = new GetJobInstanceReq();

		Map<String, JobParameterType> map = new HashMap<String, JobParameterType>();
		for(Entry<String, JobParameter> parameter : jobParameters.getParameters().entrySet()) {
			JobParameterType type = new JobParameterType();
			type.parameter = parameter.getValue().getValue();
			type.parameterType = parameter.getValue().getType();
			map.put(parameter.getKey(), type);
		}

		req.jobName = jobName;
		req.jobParameters = map;
		return req;
	}

	/**
	 * Builds request for getting a job instance by id.
	 *
	 * @param id the job instance id
	 * @return the {@link GetJobInstanceByIdReq} request
	 */
	public static GetJobInstanceByIdReq buildGetJobInstanceByIdReq(Long id) {
		GetJobInstanceByIdReq req = new GetJobInstanceByIdReq();
		req.id = id;
		return req;
	}

	/**
	 * Builds request for getting a job instance by its name
	 * and paging info.
	 *
	 * @param jobName the job name
	 * @param start index where to start
	 * @param count max number of entries to request
	 * @return the {@link GetJobInstancesReq} request
	 */
	public static GetJobInstancesReq buildGetJobInstancesReq(String jobName, int start, int count) {
		GetJobInstancesReq req = new GetJobInstancesReq();
		req.jobName = jobName;
		req.count = count;
		req.start = start;
		return req;
	}

	/**
	 * Builds request for getting a last execution by a job instance.
	 *
	 * @param jobInstance the job instance
	 * @return the {@link GetLastJobExecutionReq} request
	 */
	public static GetLastJobExecutionReq buildGetLastJobExecutionReq(JobInstance jobInstance) {
		GetLastJobExecutionReq req = new GetLastJobExecutionReq();
		req.jobInstance = convertJobInstanceType(jobInstance);
		return req;
	}

	/**
	 * Builds request for finding a job execution by a job instance.
	 *
	 * @param jobInstance the job instance
	 * @return the {@link FindJobExecutionsReq} request
	 */
	public static FindJobExecutionsReq buildFindJobExecutionsReq(JobInstance jobInstance) {
		FindJobExecutionsReq req = new FindJobExecutionsReq();
		req.jobInstance = convertJobInstanceType(jobInstance);
		return req;
	}

	/**
	 * Builds request for finding a job executions by a job name.
	 *
	 * @param jobName the job name
	 * @return the {@link FindRunningJobExecutionsReq} request
	 */
	public static FindRunningJobExecutionsReq buildFindRunningJobExecutionsReq(String jobName) {
		FindRunningJobExecutionsReq req = new FindRunningJobExecutionsReq();
		req.jobName = jobName;
		return req;
	}

	/**
	 * Builds request for getting a job execution by its execution id.
	 *
	 * @param executionId the execution id
	 * @return the {@link GetJobExecutionReq} request
	 */
	public static GetJobExecutionReq buildGetJobExecutionReq(Long executionId) {
		GetJobExecutionReq req = new GetJobExecutionReq();
		req.executionId = executionId;
		return req;
	}

	/**
	 * Builds request for getting a job names.
	 *
	 * @return the {@link GetJobNamesReq} request
	 */
	public static GetJobNamesReq buildGetJobNamesReq() {
		GetJobNamesReq req = new GetJobNamesReq();
		return req;
	}

	private static Long nullsafeToMillis(Date date) {
		if(date != null) {
			return date.getTime();
		} else {
			return null;
		}
	}

	private static Date nullsafeToDate(Long millis) {
		if(millis != null) {
			return new Date(millis);
		} else {
			return null;
		}
	}

}
