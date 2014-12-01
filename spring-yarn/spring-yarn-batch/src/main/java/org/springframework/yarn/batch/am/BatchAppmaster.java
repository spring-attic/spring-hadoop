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
package org.springframework.yarn.batch.am;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.yarn.am.AppmasterService;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.allocate.AbstractAllocator;
import org.springframework.yarn.batch.event.JobExecutionEvent;
import org.springframework.yarn.batch.partition.AbstractPartitionHandler;
import org.springframework.yarn.batch.repository.BatchAppmasterService;
//import org.springframework.yarn.batch.repository.BatchAppmasterService;
import org.springframework.yarn.batch.repository.JobRepositoryRemoteServiceInterceptor;
import org.springframework.yarn.batch.repository.JobRepositoryRpcFactory;
import org.springframework.yarn.batch.repository.bindings.PartitionedStepExecutionStatusReq;
import org.springframework.yarn.batch.repository.bindings.PartitionedStepExecutionStatusRes;
//import org.springframework.yarn.batch.repository.bindings.PartitionedStepExecutionStatusReq;
//import org.springframework.yarn.batch.repository.bindings.PartitionedStepExecutionStatusRes;
import org.springframework.yarn.batch.repository.bindings.StepExecutionType;
import org.springframework.yarn.event.AbstractYarnEvent;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

/**
 * Implementation of application master which can be used to
 * run Spring Batch jobs on Hadoop Yarn cluster.
 * <p>
 * Application master will act as a context running the Spring
 * Batch job. Order to make some sense in terms of using cluster
 * resources, job itself should be able to partition itself in
 * a way that Yarn containers can be used to split the load.
 *
 * @author Janne Valkealahti
 *
 */
public class BatchAppmaster extends AbstractBatchAppmaster implements YarnAppmaster {

	private static final Log log = LogFactory.getLog(BatchAppmaster.class);

	@Autowired(required = false)
	private final Collection<PartitionHandler> partitionHandlers = Collections.emptySet();

	private List<JobExecution> jobExecutions = new ArrayList<JobExecution>();

	@Override
	public void submitApplication() {
		log.info("submitApplication");
		registerAppmaster();
		start();
		if (getAllocator() instanceof AbstractAllocator) {
			log.info("about to set app attempt id");
			((AbstractAllocator) getAllocator()).setApplicationAttemptId(getApplicationAttemptId());
		}

		for (PartitionHandler handler : partitionHandlers) {
			if (handler instanceof AbstractPartitionHandler) {
				((AbstractPartitionHandler)handler).setBatchAppmaster(this);
			}
		}

		try {
			getYarnJobLauncher().run(getParameters());
		} catch (JobExecutionException e) {
			log.error("Error in jobLauncherHelper", e);
			setFinalApplicationStatus(FinalApplicationStatus.FAILED);
		}
		for (JobExecution jobExecution : jobExecutions) {
			if (jobExecution.getStatus().equals(BatchStatus.FAILED)) {
				setFinalApplicationStatus(FinalApplicationStatus.FAILED);
				break;
			}
		}
		notifyCompleted();
	}

	@Override
	public void onApplicationEvent(AbstractYarnEvent event) {
		super.onApplicationEvent(event);
		if (event instanceof JobExecutionEvent) {
			jobExecutions.add(((JobExecutionEvent)event).getJobExecution());
		}
	}

	@Override
	protected void doStart() {
		super.doStart();

		AppmasterService service = getAppmasterService();
		if(log.isDebugEnabled() && service != null) {
			log.debug("We have a appmaster service " + service);
		}

		if(service instanceof BatchAppmasterService) {
			((BatchAppmasterService)service).addInterceptor(new JobRepositoryRemoteServiceInterceptor() {

				@Override
				public BaseObject preRequest(BaseObject baseObject) {
					if(baseObject.getType().equals("PartitionedStepExecutionStatusReq")) {
						StepExecutionType stepExecutionType = ((PartitionedStepExecutionStatusReq)baseObject).stepExecution;
						StepExecution convertStepExecution = JobRepositoryRpcFactory.convertStepExecutionType(stepExecutionType);
						getStepExecutions().add(convertStepExecution);
						return null;
					} else {
						return baseObject;
					}
				}

				@Override
				public BaseResponseObject postRequest(BaseResponseObject baseResponseObject) {
					return baseResponseObject;
				}

				@Override
				public BaseResponseObject handleRequest(BaseObject baseObject) {
					return new PartitionedStepExecutionStatusRes();
				}
			});
		}

		if(service != null && service.hasPort()) {
			for(int i=0; i<10; i++) {
				if(service.getPort() == -1) {
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
					}
				} else {
					break;
				}
				// should fail
			}
		}

		if(getAppmasterService() instanceof SmartLifecycle) {
			((SmartLifecycle)getAppmasterService()).start();
		}

	}

	@Override
	protected void doStop() {
		// TODO we override stop here for AbstractAppmaster not
		//      to call notifyCompleted() because we currently need to call
		//      it at the end of submitApplication(). should do a little
		//      refactoring to play nice with lifecycle.
		finishAppmaster();
	}

}
