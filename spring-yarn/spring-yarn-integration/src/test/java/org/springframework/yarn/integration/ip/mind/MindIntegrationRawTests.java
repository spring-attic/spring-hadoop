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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.am.AppmasterService;
import org.springframework.yarn.client.AppmasterScOperations;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;
import org.springframework.yarn.integration.support.PortExposingTcpSocketSupport;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests for integrating {@link AppmasterService} and
 * {@link AppmasterScOperations} together. We're mostly testing that appmaster
 * service client is able to talk to appmaster service using the mind protocol.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext(classMode=ClassMode.AFTER_EACH_TEST_METHOD)
public class MindIntegrationRawTests {
	private static final Log log = LogFactory.getLog(MindIntegrationRawTests.class);

	@Autowired
	ApplicationContext ctx;

	@Autowired
	PortExposingTcpSocketSupport socketSupport;

	@Autowired
	@Qualifier("randomTestPort")
	Integer randomTestPort;

	@Autowired
	MessageChannel serverChannel;

	@Autowired
	MessageChannel clientRequestChannel;

	@Autowired
	QueueChannel clientResponseChannel;

	@Autowired
	AppmasterService mindAppmasterService;

	@Autowired
	MindAppmasterServiceClient mindAppmasterServiceClient;

	@Test
	public void testVanillaChannels() throws Exception {
		log.info("randomTestPort=" + randomTestPort);
		waitPort();

		assertNotNull(socketSupport);

		SimpleTestRequest req = new SimpleTestRequest();
		ObjectMapper objectMapper = new ObjectMapper();

		String content = objectMapper.writeValueAsString(req);

		Map<String, String> headers = new HashMap<String, String>();
		headers.put("type", "SimpleTestRequest");
		MindRpcMessageHolder holder = new MindRpcMessageHolder(headers, content);
		clientRequestChannel.send(MessageBuilder.withPayload(holder).build());

		Message<?> receive = clientResponseChannel.receive();
		holder = (MindRpcMessageHolder) receive.getPayload();
		String contentRes = new String(holder.getContent());
		assertNotNull(contentRes);
	}

	@Test
	public void testServiceInterfaces() throws Exception {
		log.info("randomTestPort=" + randomTestPort);
		waitPort();
		assertNotNull(mindAppmasterService);
		assertNotNull(mindAppmasterServiceClient);

		assertThat(mindAppmasterService.getPort(), greaterThan(0));

		SimpleTestRequest request = new SimpleTestRequest();
		BaseResponseObject response = mindAppmasterServiceClient.doMindRequest(request);
		assertNotNull(response);
	}

	private void waitPort() {
		assertNotNull(mindAppmasterService);
		for (int i = 0; i<5; i++) {
			log.info("waiting port " + i);
			if (mindAppmasterService.getPort() > 0) {
				log.info("found port " + mindAppmasterService.getPort());
				return;
			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}
		}
	}

}
