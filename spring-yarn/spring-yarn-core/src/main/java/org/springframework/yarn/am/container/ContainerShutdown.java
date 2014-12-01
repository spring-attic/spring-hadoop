/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.yarn.am.container;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;

/**
 * Generic strategy interface to shutdown containers. Implementor can
 * choose how containers are brought down which may either happen
 * by killing via node manager or i.e. via boot shutdown endpoint
 * if that is known in a info structure.
 *
 * @author Janne Valkealahti
 *
 */
public interface ContainerShutdown {

	/**
	 * Shutdown containers.
	 *
	 * @param containers the containers
	 * @return true, if shutdown is considered successful
	 */
	boolean shutdown(Map<Container, ContainerRegisterInfo> containers);

}
