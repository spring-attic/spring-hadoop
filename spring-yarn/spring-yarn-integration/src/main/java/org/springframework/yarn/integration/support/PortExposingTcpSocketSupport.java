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

import org.springframework.integration.ip.tcp.connection.support.TcpSocketSupport;

/**
 * Extension of {@link TcpSocketSupport} interface adding methods
 * to get more information about the socket ports.
 *
 * @author Janne Valkealahti
 *
 */
public interface PortExposingTcpSocketSupport extends TcpSocketSupport {

	/**
	 * Gets the binded server socket port.
	 *
	 * @return the server socket port
	 */
	int getServerSocketPort();

	/**
	 * Gets the binded server socket address.
	 *
	 * @return the server socket address
	 */
	String getServerSocketAddress();

}
