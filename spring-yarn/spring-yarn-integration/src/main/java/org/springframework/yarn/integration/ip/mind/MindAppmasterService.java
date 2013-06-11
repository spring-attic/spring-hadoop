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
package org.springframework.yarn.integration.ip.mind;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.yarn.am.AppmasterService;
import org.springframework.yarn.am.GenericRpcMessage;
import org.springframework.yarn.am.RpcMessage;
import org.springframework.yarn.integration.IntegrationAppmasterService;

/**
 * Implementation of {@link AppmasterService} which handles communication
 * via Spring Int tcp channels using mind protocol.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class MindAppmasterService extends IntegrationAppmasterService<MindRpcMessageHolder> {

	private static final Log log = LogFactory.getLog(MindAppmasterService.class);

	@Override
	public RpcMessage<MindRpcMessageHolder> handleMessageInternal(RpcMessage<MindRpcMessageHolder> message) {
		if(log.isDebugEnabled()) {
			log.debug("Handling MindRpcMessageHolder: " + message);
		}
		MindRpcMessageHolder responseMessage = handleMindMessageInternal(message.getBody());
		if(log.isDebugEnabled()) {
			log.debug("Sending response MindRpcMessageHolder: " + responseMessage);
		}
		return new GenericRpcMessage<MindRpcMessageHolder>(responseMessage);
	}

	/**
	 * Internal message handling for {@link MindRpcMessageHolder}.
	 *
	 * @param message the {@link MindRpcMessageHolder}
	 * @return response as {@link MindRpcMessageHolder}
	 */
	protected abstract MindRpcMessageHolder handleMindMessageInternal(MindRpcMessageHolder message);

}
