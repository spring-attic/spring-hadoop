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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameter.ParameterType;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.yarn.batch.repository.bindings.JobParameterType;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobNamesReq;
import org.springframework.yarn.batch.repository.bindings.exp.GetJobNamesRes;
import org.springframework.yarn.batch.repository.bindings.exp.GetStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.exp.GetStepExecutionRes;
import org.springframework.yarn.batch.repository.bindings.repo.AddWithStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.repo.AddWithStepExecutionRes;
import org.springframework.yarn.batch.repository.bindings.repo.CreateJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.repo.CreateJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.repo.GetLastJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.repo.GetLastJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.repo.GetLastStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.repo.GetLastStepExecutionRes;
import org.springframework.yarn.batch.repository.bindings.repo.GetStepExecutionCountReq;
import org.springframework.yarn.batch.repository.bindings.repo.GetStepExecutionCountRes;
import org.springframework.yarn.batch.repository.bindings.repo.IsJobInstanceExistsReq;
import org.springframework.yarn.batch.repository.bindings.repo.IsJobInstanceExistsRes;
import org.springframework.yarn.batch.repository.bindings.repo.UpdateExecutionContextReq;
import org.springframework.yarn.batch.repository.bindings.repo.UpdateExecutionContextRes;
import org.springframework.yarn.batch.repository.bindings.repo.UpdateWithJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.repo.UpdateWithJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.repo.UpdateWithStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.repo.UpdateWithStepExecutionRes;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class JobRepositoryService {

	private final static Log log = LogFactory.getLog(JobRepositoryService.class);

	private JobRepository jobRepository;
	private JobExplorer jobExplorer;

	public void setJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
	}

	public void setJobExplorer(JobExplorer jobExplorer) {
		this.jobExplorer = jobExplorer;
	}

	public BaseResponseObject get(BaseObject request) {
		BaseResponseObject responseObj = null;
		log.info("Handling rpc request for type=" + request.getType());

		if (request.getType().equals("IsJobInstanceExistsReq")) {
			responseObj = handleIsJobInstanceExists((IsJobInstanceExistsReq)request);
		} else if (request.getType().equals("CreateJobExecutionReq")) {
			responseObj = handleCreateJobExecutionReq((CreateJobExecutionReq)request);
		} else if (request.getType().equals("UpdateWithJobExecutionReq")) {
			responseObj = handleUpdateWithJobExecutionReq((UpdateWithJobExecutionReq)request);
		} else if (request.getType().equals("AddWithStepExecutionReq")) {
			responseObj = handleAddWithStepExecutionReq((AddWithStepExecutionReq)request);
		} else if (request.getType().equals("GetStepExecutionCountReq")) {
			responseObj = handleGetStepExecutionCountReq((GetStepExecutionCountReq)request);
		} else if (request.getType().equals("GetLastStepExecutionReq")) {
			responseObj = handleGetLastStepExecutionReq((GetLastStepExecutionReq)request);
		} else if (request.getType().equals("UpdateWithStepExecutionReq")) {
			responseObj = handleUpdateWithStepExecutionReq((UpdateWithStepExecutionReq)request);
		} else if (request.getType().equals("GetLastJobExecutionReq")) {
			responseObj = handleGetLastJobExecutionReq((GetLastJobExecutionReq)request);
		} else if (request.getType().equals("UpdateExecutionContextReq")) {
			responseObj = handleUpdateExecutionContext((UpdateExecutionContextReq)request);
		} else if (request.getType().equals("GetJobInstancesReq")) {
		} else if (request.getType().equals("GetJobExecutionReq")) {
		} else if (request.getType().equals("GetStepExecutionReq")) {
			responseObj = handleGetStepExecutionReq((GetStepExecutionReq)request);
		} else if (request.getType().equals("GetJobInstanceReq")) {
		} else if (request.getType().equals("GetJobExecutionsReq")) {
		} else if (request.getType().equals("FindRunningJobExecutionsReq")) {
		} else if (request.getType().equals("GetJobNamesReq")) {
			responseObj = handleGetJobNamesReq((GetJobNamesReq)request);
		}


		log.info("Handled rpc request for type=" + request.getType() + ". Returning responseObj " + responseObj);
		return responseObj;
	}

	private BaseResponseObject handleGetStepExecutionReq(GetStepExecutionReq request) {
		GetStepExecutionRes response = new GetStepExecutionRes();
		StepExecution stepExecution = jobExplorer.getStepExecution(request.jobExecutionId, request.stepExecutionId);
		response.stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(stepExecution);
		return response;
	}

	private BaseResponseObject handleGetJobNamesReq(GetJobNamesReq request) {
		GetJobNamesRes response = new GetJobNamesRes();
		response.jobNames = jobExplorer.getJobNames();
		return response;
	}

	private BaseResponseObject handleIsJobInstanceExists(IsJobInstanceExistsReq request) {
		IsJobInstanceExistsRes response = null;

		String jobName = request.jobName;

		Map<String, JobParameter> map = new HashMap<String, JobParameter>();
		for(Entry<String, JobParameterType> entry : request.jobParameters.entrySet()) {
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
		JobParameters jobParameters = new JobParameters(map);

		boolean jobInstanceExists = jobRepository.isJobInstanceExists(jobName, jobParameters);
		response = new IsJobInstanceExistsRes(jobInstanceExists);
		return response;
	}

	private BaseResponseObject handleCreateJobExecutionReq(CreateJobExecutionReq request)  {
		CreateJobExecutionRes response = null;

		String jobName = request.jobName;

		Map<String, JobParameter> map = new HashMap<String, JobParameter>();
		for(Entry<String, JobParameterType> entry : request.jobParameters.entrySet()) {
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
		JobParameters jobParameters = new JobParameters(map);
		//throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException
		JobExecution jobExecution = null;
		try {
			jobExecution = jobRepository.createJobExecution(jobName, jobParameters);
		} catch (JobExecutionAlreadyRunningException e) {
			e.printStackTrace();
		} catch (JobRestartException e) {
			e.printStackTrace();
		} catch (JobInstanceAlreadyCompleteException e) {
			e.printStackTrace();
		}
		response = new CreateJobExecutionRes();
		response.jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(jobExecution);
		return response;
	}

	private BaseResponseObject handleUpdateWithJobExecutionReq(UpdateWithJobExecutionReq request) {
		UpdateWithJobExecutionRes response = null;
		JobExecution jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(request.jobExecution);
		jobRepository.update(jobExecution);
		response = new UpdateWithJobExecutionRes();
		response.id = jobExecution.getId();
		response.version = jobExecution.getVersion();
		return response;
	}

	private BaseResponseObject handleAddWithStepExecutionReq(AddWithStepExecutionReq request) {
		AddWithStepExecutionRes response = null;
		StepExecution stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(request.stepExecution);
		jobRepository.add(stepExecution);
		response = new AddWithStepExecutionRes();
		response.id = stepExecution.getId();
		response.version = stepExecution.getVersion();
		return response;
	}

	private BaseResponseObject handleGetStepExecutionCountReq(GetStepExecutionCountReq request) {
		GetStepExecutionCountRes response = null;
		JobInstance jobInstance = JobRepositoryRpcFactory.convertJobInstanceType(request.jobInstance);
		int stepExecutionCount = jobRepository.getStepExecutionCount(jobInstance, request.stepName);
		response = new GetStepExecutionCountRes();
		response.count = stepExecutionCount;
		return response;
	}

	private BaseResponseObject handleGetLastStepExecutionReq(GetLastStepExecutionReq request) {
		GetLastStepExecutionRes response = null;
		JobInstance jobInstance = JobRepositoryRpcFactory.convertJobInstanceType(request.jobInstance);
		StepExecution lastStepExecution = jobRepository.getLastStepExecution(jobInstance, request.stepName);
		response = new GetLastStepExecutionRes();
		response.stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(lastStepExecution);
		return response;
	}

	private BaseResponseObject handleUpdateWithStepExecutionReq(UpdateWithStepExecutionReq request) {
		UpdateWithStepExecutionRes response = null;
		StepExecution stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(request.stepExecution);
		jobRepository.update(stepExecution);
		response = new UpdateWithStepExecutionRes();
		response.id = stepExecution.getId();
		response.version = stepExecution.getVersion();
		return response;
	}

	private BaseResponseObject handleGetLastJobExecutionReq(GetLastJobExecutionReq request) {
		GetLastJobExecutionRes response = null;

		String jobName = request.jobName;

		Map<String, JobParameter> map = new HashMap<String, JobParameter>();
		for(Entry<String, JobParameterType> entry : request.jobParameters.entrySet()) {
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
		JobParameters jobParameters = new JobParameters(map);

		JobExecution lastJobExecution = jobRepository.getLastJobExecution(jobName, jobParameters);

		response = new GetLastJobExecutionRes();
		response.jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(lastJobExecution);

		return response;
	}

	private BaseResponseObject handleUpdateExecutionContext(UpdateExecutionContextReq request) {
		UpdateExecutionContextRes response = null;
		try {
			if(request.stepExecution != null) {
				StepExecution stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(request.stepExecution);
				jobRepository.updateExecutionContext(stepExecution);
			} else if(request.jobExecution != null) {
				JobExecution jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(request.jobExecution);
				jobRepository.updateExecutionContext(jobExecution);
			}
			response = new UpdateExecutionContextRes();
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}


}
