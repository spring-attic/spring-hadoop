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
package org.springframework.yarn.integration.support;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.am.AppmasterService;

/**
 * Testing socket support logic through appmaster service
 * if nio sockets are used.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class PortExposingTcpSocketSupportNioTests {

	@Autowired
	AppmasterService mindAppmasterService;

	@Test
	public void testExposedSocketPort() throws Exception {
		assertNotNull(mindAppmasterService);
		// framework classes using port support are
		// waiting and polling for some amount of time
		// need to do same here
		assertThat(waitAndPollPort(), greaterThan(0));
	}

	private int waitAndPollPort() {
		int port = mindAppmasterService.getPort();
		for (int i = 0; i<10; i++) {
			if (port > 0) {
				break;
			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
				port = mindAppmasterService.getPort();
			}
		}
		return port;
	}

}
