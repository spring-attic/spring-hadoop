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

import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

/**
 * Interface for appmaster to container manager communication.
 *
 * @author Janne Valkealahti
 * @see org.springframework.yarn.am.AppmasterCmTemplate
 * @see org.apache.hadoop.yarn.api.ContainerManager
 *
 */
public interface AppmasterCmOperations {

	/**
	 * Start container.
	 *
	 * @param request the request
	 * @return the start container response
	 */
	StartContainerResponse startContainer(StartContainerRequest request);

	/**
	 * Stop container.
	 *
	 * @return the stop container response
	 */
	StopContainerResponse stopContainer();

	/**
	 * Gets the container status.
	 *
	 * @return the container status
	 */
	ContainerStatus getContainerStatus();

}
