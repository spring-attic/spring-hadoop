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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameter.ParameterType;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.yarn.batch.repository.bindings.AddStepExecutionsReq;
import org.springframework.yarn.batch.repository.bindings.AddStepExecutionsRes;
import org.springframework.yarn.batch.repository.bindings.CreateJobInstanceReq;
import org.springframework.yarn.batch.repository.bindings.CreateJobInstanceRes;
import org.springframework.yarn.batch.repository.bindings.FindJobExecutionsReq;
import org.springframework.yarn.batch.repository.bindings.FindJobExecutionsRes;
import org.springframework.yarn.batch.repository.bindings.FindRunningJobExecutionsReq;
import org.springframework.yarn.batch.repository.bindings.FindRunningJobExecutionsRes;
import org.springframework.yarn.batch.repository.bindings.GetExecutionContextReq;
import org.springframework.yarn.batch.repository.bindings.GetExecutionContextRes;
import org.springframework.yarn.batch.repository.bindings.GetJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.GetJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.GetJobInstanceByIdReq;
import org.springframework.yarn.batch.repository.bindings.GetJobInstanceByIdRes;
import org.springframework.yarn.batch.repository.bindings.GetJobInstanceReq;
import org.springframework.yarn.batch.repository.bindings.GetJobInstanceRes;
import org.springframework.yarn.batch.repository.bindings.GetJobInstancesReq;
import org.springframework.yarn.batch.repository.bindings.GetJobInstancesRes;
import org.springframework.yarn.batch.repository.bindings.GetJobNamesReq;
import org.springframework.yarn.batch.repository.bindings.GetJobNamesRes;
import org.springframework.yarn.batch.repository.bindings.GetLastJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.GetLastJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.GetStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.GetStepExecutionRes;
import org.springframework.yarn.batch.repository.bindings.JobExecutionType;
import org.springframework.yarn.batch.repository.bindings.JobInstanceType;
import org.springframework.yarn.batch.repository.bindings.JobParameterType;
import org.springframework.yarn.batch.repository.bindings.SaveExecutionContextReq;
import org.springframework.yarn.batch.repository.bindings.SaveExecutionContextRes;
import org.springframework.yarn.batch.repository.bindings.SaveJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.SaveJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.SaveStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.SaveStepExecutionRes;
import org.springframework.yarn.batch.repository.bindings.SynchronizeStatusReq;
import org.springframework.yarn.batch.repository.bindings.SynchronizeStatusRes;
import org.springframework.yarn.batch.repository.bindings.UpdateExecutionContextReq;
import org.springframework.yarn.batch.repository.bindings.UpdateExecutionContextRes;
import org.springframework.yarn.batch.repository.bindings.UpdateJobExecutionReq;
import org.springframework.yarn.batch.repository.bindings.UpdateJobExecutionRes;
import org.springframework.yarn.batch.repository.bindings.UpdateStepExecutionReq;
import org.springframework.yarn.batch.repository.bindings.UpdateStepExecutionRes;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

/**
 * Service class handling remote operations for
 * Spring Batch job repository.
 *
 * @author Janne Valkealahti
 *
 */
public class JobRepositoryRemoteService implements InitializingBean {

	private final static Log log = LogFactory.getLog(JobRepositoryRemoteService.class);

	private JobExecutionDao jobExecutionDao;
	private JobInstanceDao jobInstanceDao;
	private StepExecutionDao stepExecutionDao;
	private ExecutionContextDao executionContextDao;
	private MapJobRepositoryFactoryBean mapJobRepositoryFactoryBean;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(mapJobRepositoryFactoryBean, "MapJobRepositoryFactoryBean must not be null");
		setJobExecutionDao(mapJobRepositoryFactoryBean.getJobExecutionDao());
		setJobInstanceDao(mapJobRepositoryFactoryBean.getJobInstanceDao());
		setStepExecutionDao(mapJobRepositoryFactoryBean.getStepExecutionDao());
		setExecutionContextDao(mapJobRepositoryFactoryBean.getExecutionContextDao());
	}

	/**
	 * Sets the {@link MapJobRepositoryFactoryBean} for this service class.
	 *
	 * @param mapJobRepositoryFactoryBean the {@link MapJobRepositoryFactoryBean}
	 */
	public void setMapJobRepositoryFactoryBean(MapJobRepositoryFactoryBean mapJobRepositoryFactoryBean) {
		this.mapJobRepositoryFactoryBean = mapJobRepositoryFactoryBean;
	}

	/**
	 * Sets the {@link JobExecutionDao} for this service class.
	 *
	 * @param jobExecutionDao the {@link JobExecutionDao}
	 */
	public void setJobExecutionDao(JobExecutionDao jobExecutionDao) {
		this.jobExecutionDao = jobExecutionDao;
	}

	/**
	 * Sets the {@link JobInstanceDao} for this service class.
	 *
	 * @param jobInstanceDao the {@link JobInstanceDao}
	 */
	public void setJobInstanceDao(JobInstanceDao jobInstanceDao) {
		this.jobInstanceDao = jobInstanceDao;
	}

	/**
	 * Sets the {@link StepExecutionDao} for this service class.
	 *
	 * @param stepExecutionDao the {@link StepExecutionDao}
	 */
	public void setStepExecutionDao(StepExecutionDao stepExecutionDao) {
		this.stepExecutionDao = stepExecutionDao;
	}

	/**
	 * Sets the {@link ExecutionContextDao} for this service class.
	 *
	 * @param executionContextDao the {@link ExecutionContextDao}
	 */
	public void setExecutionContextDao(ExecutionContextDao executionContextDao) {
		this.executionContextDao = executionContextDao;
	}

	/**
	 * Handles requests.
	 *
	 * @param request the base object request.
	 * @return response as of type {@link BaseResponseObject}
	 */
	public BaseResponseObject get(BaseObject request) {

		BaseResponseObject responseObj = null;

		if(log.isDebugEnabled()) {
			log.debug("Handling rpc request for type=" + request.getType());
		}

		if (request.getType().equals("CreateJobInstanceReq")) {
			responseObj = handleCreateJobInstance((CreateJobInstanceReq)request);
		} else if (request.getType().equals("GetJobInstanceReq")) {
			responseObj = handleGetJobInstance((GetJobInstanceReq)request);
		} else if (request.getType().equals("GetJobInstanceByIdReq")) {
			responseObj = handleGetJobInstanceById((GetJobInstanceByIdReq)request);
		} else if (request.getType().equals("GetJobInstancesReq")) {
			responseObj = handleGetJobInstances((GetJobInstancesReq)request);
		} else if (request.getType().equals("GetJobNamesReq")) {
			responseObj = handleGetJobNames((GetJobNamesReq)request);
		} else if (request.getType().equals("SaveStepExecutionReq")) {
			responseObj = handleSaveStepExecution((SaveStepExecutionReq)request);
		} else if (request.getType().equals("AddStepExecutionsReq")) {
			responseObj = handleAddStepExecutions((AddStepExecutionsReq)request);
		} else if (request.getType().equals("UpdateStepExecutionReq")) {
			responseObj = handleUpdateStepExecution((UpdateStepExecutionReq)request);
		} else if (request.getType().equals("GetStepExecutionReq")) {
			responseObj = handleGetStepExecution((GetStepExecutionReq)request);
		} else if (request.getType().equals("SaveJobExecutionReq")) {
			responseObj = handleSaveJobExecution((SaveJobExecutionReq)request);
		} else if (request.getType().equals("SaveExecutionContextReq")) {
			responseObj = handleSaveExecutionContext((SaveExecutionContextReq)request);
		} else if (request.getType().equals("UpdateExecutionContextReq")) {
			responseObj = handleUpdateExecutionContext((UpdateExecutionContextReq)request);
		} else if (request.getType().equals("FindJobExecutionsReq")) {
			responseObj = handleFindJobExecutions((FindJobExecutionsReq)request);
		} else if (request.getType().equals("FindRunningJobExecutionsReq")) {
			responseObj = handleFindRunningJobExecutions((FindRunningJobExecutionsReq)request);
		} else if (request.getType().equals("GetJobExecutionReq")) {
			responseObj = handleGetJobExecution((GetJobExecutionReq)request);
		} else if (request.getType().equals("GetLastJobExecutionReq")) {
			responseObj = handleGetLastJobExecution((GetLastJobExecutionReq)request);
		} else if (request.getType().equals("UpdateJobExecutionReq")) {
			responseObj = handleUpdateJobExecution((UpdateJobExecutionReq)request);
		} else if (request.getType().equals("SynchronizeStatusReq")) {
			responseObj = handleSynchronizeStatus((SynchronizeStatusReq)request);
		} else if (request.getType().equals("GetExecutionContextReq")) {
			responseObj = handleGetExecutionContext((GetExecutionContextReq)request);
		}

//		if(responseObj == null) {
//			throw new RuntimeException("Error finding defined rpc request type");
//		}

		if(log.isDebugEnabled()) {
			log.debug("Handled rpc request for type=" + request.getType() + ". Returning responseObj " + responseObj);
		}

		return responseObj;
	}

	/**
	 * Handles creating a job instance.
	 *
	 * @param request the {@link CreateJobInstanceReq}
	 * @return the {@link CreateJobInstanceRes}
	 */
	private CreateJobInstanceRes handleCreateJobInstance(CreateJobInstanceReq request) {
		CreateJobInstanceRes response = null;
		try {
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
			JobInstance createJobInstance = jobInstanceDao.createJobInstance(jobName, jobParameters);

			JobInstanceType buildJobInstanceType = JobRepositoryRpcFactory.convertJobInstanceType(createJobInstance);
			response = new CreateJobInstanceRes(buildJobInstanceType);
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles getting a job instance.
	 *
	 * @param request the {@link GetJobInstanceReq}
	 * @return the {@link GetJobInstanceRes}
	 */
	private GetJobInstanceRes handleGetJobInstance(GetJobInstanceReq request) {
		GetJobInstanceRes response = null;
		try {
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
					map.put(entry.getKey(), new JobParameter((Long)entry.getValue().parameter));
				} else if(parameterType == ParameterType.STRING) {
					map.put(entry.getKey(), new JobParameter((String)entry.getValue().parameter));
				}
			}

			JobParameters jobParameters = new JobParameters(map);
			JobInstance jobInstance = jobInstanceDao.getJobInstance(jobName, jobParameters);

			response = new GetJobInstanceRes();
			if(jobInstance != null) {
				response.jobInstance = JobRepositoryRpcFactory.convertJobInstanceType(jobInstance);
			}
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;

	}

	/**
	 * Handles getting job instance by id.
	 *
	 * @param request the {@link GetJobInstanceByIdReq}
	 * @return the {@link GetJobInstanceByIdRes}
	 */
	private GetJobInstanceByIdRes handleGetJobInstanceById(GetJobInstanceByIdReq request) {
		GetJobInstanceByIdRes response = null;
		try {
			Long id = request.id;
			JobInstance jobInstance = jobInstanceDao.getJobInstance(id);
			response = new GetJobInstanceByIdRes();
			if(jobInstance != null) {
				response.jobInstance = JobRepositoryRpcFactory.convertJobInstanceType(jobInstance);
			}
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles getting a job names.
	 *
	 * @param request the {@link GetJobNamesReq}
	 * @return the {@link GetJobNamesRes}
	 */
	private GetJobNamesRes handleGetJobNames(GetJobNamesReq request) {
		GetJobNamesRes response = null;
		try {
			List<String> jobNames = jobInstanceDao.getJobNames();
			response = new GetJobNamesRes(jobNames);
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles getting a job instances.
	 *
	 * @param request the {@link GetJobInstancesReq}
	 * @return the {@link GetJobInstancesRes}
	 */
	private GetJobInstancesRes handleGetJobInstances(GetJobInstancesReq request) {
		GetJobInstancesRes response = null;
		try {
			String jobName = request.jobName;
			Integer start = request.start;
			Integer count = request.count;
			List<JobInstance> jobInstances = jobInstanceDao.getJobInstances(jobName, start, count);

			List<JobInstanceType> jobInstanceTypes = new ArrayList<JobInstanceType>();
			for(JobInstance jobInstance : jobInstances) {
				jobInstanceTypes.add(JobRepositoryRpcFactory.convertJobInstanceType(jobInstance));
			}
			response = new GetJobInstancesRes(jobInstanceTypes);
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles saving an execution context.
	 *
	 * @param request the {@link SaveExecutionContextReq}
	 * @return the {@link SaveExecutionContextRes}
	 */
	private SaveExecutionContextRes handleSaveExecutionContext(SaveExecutionContextReq request) {
		SaveExecutionContextRes response = null;
		try {
			if(request.stepExecution != null) {
				StepExecution stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(request.stepExecution);
				executionContextDao.saveExecutionContext(stepExecution);
			} else if(request.jobExecution != null) {
				JobExecution jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(request.jobExecution);
				executionContextDao.saveExecutionContext(jobExecution);
			}
			response = new SaveExecutionContextRes();
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles getting an execution context.
	 *
	 * @param request the {@link GetExecutionContextReq}
	 * @return the {@link GetExecutionContextRes}
	 */
	private GetExecutionContextRes handleGetExecutionContext(GetExecutionContextReq request) {
		GetExecutionContextRes response = null;
		try {
			ExecutionContext executionContext = null;
			if(request.stepExecution != null) {
				StepExecution stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(request.stepExecution);
				executionContext = executionContextDao.getExecutionContext(stepExecution);
			} else if(request.jobExecution != null) {
				JobExecution jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(request.jobExecution);
				executionContext = executionContextDao.getExecutionContext(jobExecution);
			}

			response = new GetExecutionContextRes();
			response.executionContext = JobRepositoryRpcFactory.convertExecutionContext(executionContext);
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles updating an execution context.
	 *
	 * @param request the {@link UpdateExecutionContextReq}
	 * @return the {@link UpdateExecutionContextRes}
	 */
	private UpdateExecutionContextRes handleUpdateExecutionContext(UpdateExecutionContextReq request) {
		UpdateExecutionContextRes response = null;
		try {
			if(request.stepExecution != null) {
				StepExecution stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(request.stepExecution);
				executionContextDao.saveExecutionContext(stepExecution);
			} else if(request.jobExecution != null) {
				JobExecution jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(request.jobExecution);
				executionContextDao.saveExecutionContext(jobExecution);
			}
			response = new UpdateExecutionContextRes();
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles saving a job executions.
	 *
	 * @param request the {@link FindJobExecutionsReq}
	 * @return the {@link FindJobExecutionsRes}
	 */
	private FindJobExecutionsRes handleFindJobExecutions(FindJobExecutionsReq request) {
		FindJobExecutionsRes response = null;
		try {
			JobInstance jobInstance = JobRepositoryRpcFactory.convertJobInstanceType(request.jobInstance);
			List<JobExecution> jobExecutions = jobExecutionDao.findJobExecutions(jobInstance);
			response = new FindJobExecutionsRes();
			response.jobExecutions = new ArrayList<JobExecutionType>();
			for(JobExecution jobExecution : jobExecutions) {
				response.jobExecutions.add(JobRepositoryRpcFactory.convertJobExecutionType(jobExecution));
			}
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles saving a job execution.
	 *
	 * @param request the {@link SaveJobExecutionReq}
	 * @return the {@link SaveJobExecutionRes}
	 */
	private SaveJobExecutionRes handleSaveJobExecution(SaveJobExecutionReq request) {
		SaveJobExecutionRes response = null;
		try {
			JobExecution jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(request.jobExecution);
			jobExecutionDao.saveJobExecution(jobExecution);
			response = new SaveJobExecutionRes();
			response.id = jobExecution.getId();
			response.version = jobExecution.getVersion();
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles updating a job execution.
	 *
	 * @param request the {@link UpdateJobExecutionReq}
	 * @return the {@link UpdateJobExecutionRes}
	 */
	private UpdateJobExecutionRes handleUpdateJobExecution(UpdateJobExecutionReq request) {
		UpdateJobExecutionRes response = null;
		try {
			JobExecution jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(request.jobExecution);
			jobExecutionDao.updateJobExecution(jobExecution);
			response = new UpdateJobExecutionRes(jobExecution.getVersion());
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles getting a last job execution.
	 *
	 * @param request the {@link GetLastJobExecutionReq}
	 * @return the {@link GetLastJobExecutionRes}
	 */
	private GetLastJobExecutionRes handleGetLastJobExecution(GetLastJobExecutionReq request) {
		GetLastJobExecutionRes response = null;
		try {
			JobInstance jobInstance = JobRepositoryRpcFactory.convertJobInstanceType(request.jobInstance);
			JobExecution jobExecution = jobExecutionDao.getLastJobExecution(jobInstance);
			response = new GetLastJobExecutionRes();
			if(jobExecution != null) {
				response.jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(jobExecution);
			}
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles finding running job executions.
	 *
	 * @param request the {@link FindRunningJobExecutionsReq}
	 * @return the {@link FindRunningJobExecutionsRes}
	 */
	private FindRunningJobExecutionsRes handleFindRunningJobExecutions(FindRunningJobExecutionsReq request) {
		FindRunningJobExecutionsRes response = null;
		try {
			Set<JobExecution> jobExecutions = jobExecutionDao.findRunningJobExecutions(request.jobName);
			response = new FindRunningJobExecutionsRes();
			response.jobExecutions = new HashSet<JobExecutionType>();
			for(JobExecution jobExecution : jobExecutions) {
				response.jobExecutions.add(JobRepositoryRpcFactory.convertJobExecutionType(jobExecution));
			}
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles getting a job execution.
	 *
	 * @param request the {@link GetJobExecutionReq}
	 * @return the {@link GetJobExecutionRes}
	 */
	private GetJobExecutionRes handleGetJobExecution(GetJobExecutionReq request) {
		GetJobExecutionRes response = null;
		try {
			JobExecution jobExecution = jobExecutionDao.getJobExecution(request.executionId);
			response = new GetJobExecutionRes();
			if(jobExecution != null) {
				response.jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(jobExecution);
			}
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles synchronizing a job execution.
	 *
	 * @param request the {@link SynchronizeStatusReq}
	 * @return the {@link SynchronizeStatusRes}
	 */
	private SynchronizeStatusRes handleSynchronizeStatus(SynchronizeStatusReq request) {
		SynchronizeStatusRes response = null;
		try {
			JobExecution jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(request.jobExecution);
			jobExecutionDao.synchronizeStatus(jobExecution);
			response = new SynchronizeStatusRes(jobExecution.getVersion(), jobExecution.getStatus());
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles saving a step execution.
	 *
	 * @param request the {@link SaveStepExecutionReq}
	 * @return the {@link SaveStepExecutionRes}
	 */
	private SaveStepExecutionRes handleSaveStepExecution(SaveStepExecutionReq request) {
		SaveStepExecutionRes response = null;
		try {
			StepExecution stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(request.stepExecution);
			stepExecutionDao.saveStepExecution(stepExecution);
			response = new SaveStepExecutionRes(stepExecution.getId(), stepExecution.getVersion());
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles updating a step execution.
	 *
	 * @param request the {@link UpdateStepExecutionReq}
	 * @return the {@link UpdateStepExecutionRes}
	 */
	private UpdateStepExecutionRes handleUpdateStepExecution(UpdateStepExecutionReq request) {
		UpdateStepExecutionRes response = null;
		try {
			StepExecution stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(request.stepExecution);
			stepExecutionDao.updateStepExecution(stepExecution);
			response = new UpdateStepExecutionRes(stepExecution.getId(), stepExecution.getVersion());
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles getting a step execution.
	 *
	 * @param request the {@link GetStepExecutionReq}
	 * @return the {@link GetStepExecutionRes}
	 */
	private GetStepExecutionRes handleGetStepExecution(GetStepExecutionReq request) {
		GetStepExecutionRes response = null;
		try {
			JobExecution jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(request.jobExecution);
			response = new GetStepExecutionRes();
			if(request.stepExecutionId != null) {
				StepExecution stepExecution = stepExecutionDao.getStepExecution(jobExecution, request.stepExecutionId);
				if(stepExecution != null) {
					response.stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(stepExecution);
				}
			}
		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

	/**
	 * Handles adding a step executions.
	 *
	 * @param request the {@link AddStepExecutionsReq}
	 * @return the {@link AddStepExecutionsRes}
	 */
	private AddStepExecutionsRes handleAddStepExecutions(AddStepExecutionsReq request) {
		AddStepExecutionsRes response = null;
		try {
			JobExecution jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(request.jobExecution);
			stepExecutionDao.addStepExecutions(jobExecution);
			response = new AddStepExecutionsRes();
			response.jobExecution = JobRepositoryRpcFactory.convertJobExecutionType(jobExecution);
			// step executions inside job executions doesn't get set

		} catch (Exception e) {
			log.error("error handling command", e);
		}
		return response;
	}

}
