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
package org.springframework.yarn.client;

import org.springframework.yarn.am.RpcMessage;

/**
 * Interface defining message operations for Application
 * Master Service Client.
 *
 * @author Janne Valkealahti
 *
 */
public interface AppmasterScOperations {

	/**
	 * Sends request wrapped in {@link RpcMessage} and
	 * receives response similarly wrapped in {@link RpcMessage}.
	 *
	 * @param message the {@link RpcMessage} request
	 * @return the {@link RpcMessage} response
	 */
	RpcMessage<?> get(RpcMessage<?> message);

}
