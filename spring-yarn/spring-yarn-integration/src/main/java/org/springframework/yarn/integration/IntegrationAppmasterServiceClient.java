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
package org.springframework.yarn.integration;

import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.yarn.am.AppmasterServiceClient;
import org.springframework.yarn.am.GenericRpcMessage;
import org.springframework.yarn.am.RpcMessage;
import org.springframework.yarn.client.AppmasterScOperations;
import org.springframework.yarn.integration.support.IntegrationObjectSupport;

/**
 * Implementation of Appmaster service client working on top of
 * Spring Int tcp channels.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class IntegrationAppmasterServiceClient<T> extends IntegrationObjectSupport implements
		AppmasterScOperations, AppmasterServiceClient {

	/** Outgoing request channel */
	private MessageChannel requestChannel;

	/** Incoming response channel */
	private PollableChannel responseChannel;

	/**
	 * Set the request channel for outgoing messages.
	 *
	 * @param requestChannel the request channel
	 */
	public void setRequestChannel(MessageChannel requestChannel) {
		this.requestChannel = requestChannel;
	}

	/**
	 * Set the response channel for incoming messages.
	 *
	 * @param responseChannel the response channel
	 */
	public void setResponseChannel(PollableChannel responseChannel) {
		this.responseChannel = responseChannel;
	}

	@SuppressWarnings("unchecked")
	@Override
	public RpcMessage<?> get(RpcMessage<?> message) {
		Message<T> outPayload = MessageBuilder.withPayload(getPayload(message)).build();
		requestChannel.send(outPayload);
		T payload = (T) responseChannel.receive().getPayload();
		return new GenericRpcMessage<T>(payload);
	}

	/**
	 * This method is called from {@link #get(RpcMessage)} to
	 * resolve the actual payload sent to Sprint Int Tcp channel.
	 * In this class we don't care what the payload content is, thus
	 * implementor is responsible to define it.
	 *
	 * @param message the rpc message
	 * @return the payload
	 */
	protected abstract T getPayload(RpcMessage<?> message);

}
