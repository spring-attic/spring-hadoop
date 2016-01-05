/*
 * Copyright 2013-2016 the original author or authors.
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
package org.springframework.yarn.am;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.context.SmartLifecycle;
import org.springframework.yarn.am.allocate.AbstractAllocator;
import org.springframework.yarn.listener.ContainerMonitorListener;

/**
 * A simple application master implementation which will allocate
 * and launch a number of containers, monitor container statuses
 * and finally exit the application by sending corresponding
 * message back to resource manager.
 *
 * @author Janne Valkealahti
 *
 */
public class StaticAppmaster extends AbstractProcessingAppmaster implements YarnAppmaster {

	private static final Log log = LogFactory.getLog(StaticAppmaster.class);

	/** Static count of containers to run */
	private int containerCount;

	@Override
	public void submitApplication() {
		log.info("Submitting application");
		registerAppmaster();
		start();
		if(getAllocator() instanceof AbstractAllocator) {
			((AbstractAllocator)getAllocator()).setApplicationAttemptId(getApplicationAttemptId());
		}
		containerCount = Integer.parseInt(getParameters().getProperty(AppmasterConstants.CONTAINER_COUNT, "1"));
		log.info("count: " + containerCount);
		getAllocator().allocateContainers(containerCount);
	}

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		getMonitor().addContainerMonitorStateListener(new ContainerMonitorListener() {
			@Override
			public void state(ContainerMonitorState state) {
				if (log.isDebugEnabled()) {
					log.debug("Received monitor state " + state);
				}
				if (getMonitor().freeCount() == 0) {
					int completed = state.getCompleted();
					int failed = state.getFailed();
					if (completed + failed >= containerCount) {
						if (failed > 0) {
							setFinalApplicationStatus(FinalApplicationStatus.FAILED);
						}
						notifyCompleted();
					}
				}
			}
		});
	}

	@Override
	protected void doStart() {
		super.doStart();
		AppmasterService service = getAppmasterService();
		if (service != null) {
			log.info("AppmasterService implementation is " + service);
		}
		if(getAppmasterService() instanceof SmartLifecycle) {
			((SmartLifecycle)getAppmasterService()).start();
		}
	}

}
