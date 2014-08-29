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
package org.springframework.yarn.batch.container;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.step.NoSuchStepException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.AppmasterServiceClient;
import org.springframework.yarn.batch.repository.JobRepositoryRpcFactory;
import org.springframework.yarn.batch.repository.bindings.PartitionedStepExecutionStatusReq;
import org.springframework.yarn.integration.IntegrationAppmasterServiceClient;
import org.springframework.yarn.integration.ip.mind.MindAppmasterServiceClient;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

/**
 * Default implementation of {@link AbstractBatchYarnContainer}
 * to handle Spring Batch remote steps.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultBatchYarnContainer extends AbstractBatchYarnContainer {

	private static final Log log = LogFactory.getLog(DefaultBatchYarnContainer.class);

	@Autowired(required=false)
	public void setAppmasterServiceClient(AppmasterServiceClient appmasterServiceClient) {
		super.setIntegrationServiceClient((IntegrationAppmasterServiceClient<?>) appmasterServiceClient);
	}

	@Override
	protected void runInternal() {

		Long jobExecutionId = safeParse(getEnvironment(YarnSystemConstants.AMSERVICE_BATCH_JOBEXECUTIONID));
		Long stepExecutionId = safeParse(getEnvironment(YarnSystemConstants.AMSERVICE_BATCH_STEPEXECUTIONID));
		String stepName = getEnvironment(YarnSystemConstants.AMSERVICE_BATCH_STEPNAME);

		if(log.isDebugEnabled()) {
			log.debug("Requesting StepExecution: " + jobExecutionId + " / " + stepExecutionId);
		}

		StepExecution stepExecution = getJobExplorer().getStepExecution(jobExecutionId, stepExecutionId);
		if (stepExecution == null) {
			throw new NoSuchStepException("No StepExecution could be located for this request: ");
		}

		if(log.isDebugEnabled()) {
			log.debug("Got StepExecution: " + stepExecution);
			log.debug("Locating Step: " + stepName);
		}

		Step step = getStepLocator().getStep(stepName);
		if(log.isDebugEnabled()) {
			log.debug("Located step: " + step);
		}

		if (step == null) {
			throw new NoSuchStepException(String.format("No Step with name [%s] could be located.", stepName));
		}

		try {
			if(log.isDebugEnabled()) {
				log.debug("Executing step: " + step + " / " + stepExecution);
			}
			step.execute(stepExecution);
		} catch (JobInterruptedException e) {
			log.error("error executing step 1", e);
			stepExecution.setStatus(BatchStatus.STOPPED);
		} catch (Throwable e) {
			log.error("error executing step 2", e);
			stepExecution.addFailureException(e);
			stepExecution.setStatus(BatchStatus.FAILED);
		}

		if(log.isDebugEnabled()) {
			log.info("Finished remote step stepExecution=[" + stepExecution + "]");
		}

		MindAppmasterServiceClient client = (MindAppmasterServiceClient) getIntegrationServiceClient();
		PartitionedStepExecutionStatusReq req = new PartitionedStepExecutionStatusReq();
		req.stepExecution = JobRepositoryRpcFactory.convertStepExecutionType(stepExecution);
		BaseResponseObject doMindRequest = client.doMindRequest(req);
		log.info("got response for status update: " + doMindRequest);
	}

	private Long safeParse(String longString) {
		try {
			return Long.parseLong(longString);
		} catch (Exception e) {
		}
		return null;
	}

}
