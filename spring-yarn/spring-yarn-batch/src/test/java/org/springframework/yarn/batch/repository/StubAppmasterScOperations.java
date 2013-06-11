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

import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.yarn.am.GenericRpcMessage;
import org.springframework.yarn.am.RpcMessage;
import org.springframework.yarn.integration.ip.mind.AppmasterMindScOperations;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

/**
 * Faking appmaster service client operations for
 * batch job repository access using in-memory
 * map based repository.
 *
 * @author Janne Valkealahti
 *
 */
public class StubAppmasterScOperations implements AppmasterMindScOperations {

	private JobExecutionDao jobExecutionDao;
	private JobInstanceDao jobInstanceDao;
	private StepExecutionDao stepExecutionDao;
	private ExecutionContextDao executionContextDao;

	private JobRepositoryRemoteService jobRepositoryRemoteService;

	public StubAppmasterScOperations() {
		MapJobRepositoryFactoryBean factory = new MapJobRepositoryFactoryBean();
		try {
			factory.afterPropertiesSet();
			jobExecutionDao = factory.getJobExecutionDao();
			jobInstanceDao = factory.getJobInstanceDao();
			stepExecutionDao = factory.getStepExecutionDao();
			executionContextDao = factory.getExecutionContextDao();
			jobRepositoryRemoteService = new JobRepositoryRemoteService();
			jobRepositoryRemoteService.setJobExecutionDao(jobExecutionDao);
			jobRepositoryRemoteService.setJobInstanceDao(jobInstanceDao);
			jobRepositoryRemoteService.setStepExecutionDao(stepExecutionDao);
			jobRepositoryRemoteService.setExecutionContextDao(executionContextDao);
			jobRepositoryRemoteService.afterPropertiesSet();
		} catch (Exception e) {
		}
	}

	@Override
	public RpcMessage<?> get(RpcMessage<?> message) {
		BaseObject baseObject = (BaseObject) message.getBody();
		BaseResponseObject baseResponseObject = jobRepositoryRemoteService.get(baseObject);
		return new GenericRpcMessage<BaseResponseObject>(baseResponseObject);
	}

	@Override
	public BaseResponseObject doMindRequest(BaseObject request) {
		RpcMessage<?> message = new GenericRpcMessage<BaseObject>(request);
		RpcMessage<?> rpcMessage = get(message);
		BaseResponseObject baseResponseObject = (BaseResponseObject) rpcMessage.getBody();
		return baseResponseObject;
	}

}
