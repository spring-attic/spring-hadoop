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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import javax.net.SocketFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.ip.tcp.TcpInboundGateway;
import org.springframework.integration.ip.tcp.connection.AbstractServerConnectionFactory;
import org.springframework.integration.ip.tcp.connection.TcpSocketSupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Tests for concepts around tricks to use random server bind port.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class ServerBindTests {

	@Autowired
	private ApplicationContext ctx;

	@Autowired
	@Qualifier(value = "javaSerialServer")
	AbstractServerConnectionFactory javaSerialServer;

	@Autowired
	@Qualifier(value = "gatewaySerialized")
	TcpInboundGateway gatewaySerialized;

	@Test
	public void testSerialized() throws Exception {
		TestTcpSocketSupport socketSupport = (TestTcpSocketSupport) ctx.getBean("socketSupport");
		waitListening(gatewaySerialized);
		assertThat(socketSupport.getLocalPort(), greaterThan(0));
		Socket socket = SocketFactory.getDefault().createSocket("localhost", socketSupport.getLocalPort());
		serializedGuts(socket);
	}

	private void waitListening(TcpInboundGateway gateway) throws Exception {
		int n = 0;
		while (!gateway.isListening()) {
			Thread.sleep(100);
			if (n++ > 100) {
				throw new Exception("Gateway failed to listen");
			}
		}
	}

	private void serializedGuts(Socket socket) throws SocketException, IOException, ClassNotFoundException {
		socket.setSoTimeout(5000);
		String greetings = "Hello World!";
		new ObjectOutputStream(socket.getOutputStream()).writeObject(greetings);
		String echo = (String) new ObjectInputStream(socket.getInputStream()).readObject();
		assertThat("echo:" + greetings, is(echo));
	}

	public static class TestTcpSocketSupport implements TcpSocketSupport {
		int port = -1;

		@Override
		public void postProcessServerSocket(ServerSocket serverSocket) {
			port = serverSocket.getLocalPort();
		}

		@Override
		public void postProcessSocket(Socket socket) {
		}

		public int getLocalPort() {
			return port;
		}
	}

	public static class TestService {
		public String test(byte[] bytes) {
			return "echo:" + new String(bytes);
		}
		public String test(String s) {
			return "echo:" + s;
		}
	}

}
