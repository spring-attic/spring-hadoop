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
package org.springframework.yarn.am;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

/**
 * Interface for appmaster to resource manager communication.
 *
 * @author Janne Valkealahti
 * @see org.springframework.yarn.am.AppmasterRmTemplate
 * @see org.apache.hadoop.yarn.api.AMRMProtocol
 *
 */
public interface AppmasterRmOperations {

	/**
	 * Register application master.
	 *
	 * @param appAttemptId the app attempt id
	 * @param host the host
	 * @param rpcPort the rpc port
	 * @param trackUrl the track url
	 * @return the register application master response
	 */
	RegisterApplicationMasterResponse registerApplicationMaster(ApplicationAttemptId appAttemptId,
			String host, Integer rpcPort, String trackUrl);

	/**
	 * Allocate container.
	 *
	 * @param request the request
	 * @return the allocate response
	 */
	AllocateResponse allocate(AllocateRequest request);

	/**
	 * Finish the application master.
	 *
	 * @param request the request
	 * @return the finish application master response
	 */
	FinishApplicationMasterResponse finish(FinishApplicationMasterRequest request);

}
