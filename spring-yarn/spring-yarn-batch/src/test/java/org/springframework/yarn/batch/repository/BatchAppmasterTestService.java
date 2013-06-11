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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.yarn.integration.ip.mind.MindAppmasterService;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

/**
 * Application master service implementation which is used for
 * remote Spring Batch steps to talk to Job Repository. Simply
 * passes requests to {@link JobRepositoryRemoteService}.
 *
 * @author Janne Valkealahti
 *
 */
public class BatchAppmasterTestService extends MindAppmasterService {

	private static final Log log = LogFactory.getLog(BatchAppmasterTestService.class);

	private JobRepositoryRemoteService jobRepositoryRemoteService;

	@Override
	protected MindRpcMessageHolder handleMindMessageInternal(MindRpcMessageHolder message) {
		if(log.isDebugEnabled()) {
			log.debug("Incoming MindRpcMessageHolder: " + message);
		}

		BaseObject baseObject = getConversionService().convert(message, BaseObject.class);
		BaseResponseObject baseResponseObject = jobRepositoryRemoteService.get(baseObject);
		MindRpcMessageHolder out = getConversionService().convert(baseResponseObject, MindRpcMessageHolder.class);

		if(log.isDebugEnabled()) {
			log.debug("Outgoing MindRpcMessageHolder: " + message);
		}

		return out;
	}

	public void setJobRepositoryRemoteService(JobRepositoryRemoteService jobRepositoryRemoteService) {
		this.jobRepositoryRemoteService = jobRepositoryRemoteService;
	}


}
