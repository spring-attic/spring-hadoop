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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageDeliveryException;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;
import org.springframework.yarn.am.AppmasterService;
import org.springframework.yarn.am.GenericRpcMessage;
import org.springframework.yarn.am.RpcMessage;
import org.springframework.yarn.integration.support.IntegrationObjectSupport;
import org.springframework.yarn.integration.support.PortExposingTcpSocketSupport;

/**
 * Base implementation of {@link AppmasterService} using Spring Integration Ip
 * channels as a communication link.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class IntegrationAppmasterService<T> extends IntegrationObjectSupport implements AppmasterService {

	private static final Log log = LogFactory.getLog(IntegrationAppmasterService.class);

	/** Interface needed to find info about the socket */
	private PortExposingTcpSocketSupport socketSupport;

	/** Channel where incoming messages are published */
	private SubscribableChannel messageChannel;

	/** Spring Int messaging template */
	private final MessagingTemplate messagingTemplate;

	/** Default message consumer if no custom configuration */
	private EventDrivenConsumer consumer;

	public IntegrationAppmasterService() {
		messagingTemplate = new MessagingTemplate();
	}

	@Override
	protected void doStart() {
		consumer = new EventDrivenConsumer(messageChannel, new ReplyProducingHandler());
		consumer.start();
	}

	@Override
	protected void doStop() {
		if(consumer != null) {
			consumer.stop();
		}
	}

	@Override
	public int getPort() {
		return socketSupport != null ? socketSupport.getServerSocketPort() : -1;
	}

	@Override
	public String getHost() {
		return socketSupport != null ? socketSupport.getServerSocketAddress() : null;
	}

	@Override
	public boolean hasPort() {
		return true;
	}

	/**
	 * Implementor need to write this method to process
	 * incoming messages.
	 *
	 * @param message the rpc message wrapping a protocol content
	 * @return a reply rpc message
	 */
	public abstract RpcMessage<T> handleMessageInternal(RpcMessage<T> message);

	/**
	 * Sets the message channel where messages are dispatched.
	 *
	 * @param messageChannel the message channel
	 */
	public void setMessageChannel(SubscribableChannel messageChannel) {
		Assert.notNull(messageChannel, "messageChannel must not be null");
		this.messageChannel = messageChannel;
	}

	/**
	 * Sets the socket support for this service.
	 *
	 * @param socketSupport the socket support
	 */
	public void setSocketSupport(PortExposingTcpSocketSupport socketSupport) {
		Assert.notNull(socketSupport, "socketSupport must not be null");
		this.socketSupport = socketSupport;
		if(log.isDebugEnabled()) {
			log.debug("Setting socket support: " + socketSupport);
		}
	}

	/**
	 * Send the message to the given channel. The channel must be a String or
	 * {@link MessageChannel} instance, never <code>null</code>.
	 *
	 * @param message the Spring Int message
	 * @param channel the channel to send to
	 */
	private void sendMessage(final Message<?> message, final Object channel) {
		if (channel instanceof MessageChannel) {
			this.messagingTemplate.send((MessageChannel) channel, message);
		} else if (channel instanceof String) {
			this.messagingTemplate.send((String) channel, message);
		} else {
			throw new MessageDeliveryException(message,
					"a non-null reply channel value of type MessageChannel or String is required");
		}
	}

	/**
	 * Internal default message handler which is used to handle
	 * wrapping of {@link RpcMessage} inside a {@link Message}.
	 * Gets the actual user facing message from a method and tries
	 * to provide reply to a reply channel. This handler is
	 * not necessarily used if user used custom configuration.
	 */
	private class ReplyProducingHandler implements MessageHandler {

		@SuppressWarnings("unchecked")
		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			GenericRpcMessage<T> incoming = new GenericRpcMessage<T>((T) message.getPayload());
			RpcMessage<T> outgoing = handleMessageInternal(incoming);
			Message<?> reply = MessageBuilder.withPayload(outgoing.getBody()).build();
			sendMessage(reply, message.getHeaders().getReplyChannel());
		}

	}

}
