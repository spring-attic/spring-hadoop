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

/**
 * Interface for service running on Application Master.
 * <p>
 * Usually this service provides a simple communication
 * api for client containers to connect to.
 *
 * @author Janne Valkealahti
 *
 */
public interface AppmasterService {

	/**
	 * Get a port where service is running. This method
	 * should return port as zero or negative if port is
	 * unknown. For example if underlying communication
	 * library is using random ports or other methods so
	 * that user doesn't need to worry about it.
	 *
	 * @return Service port, -1 if port unknown.
	 */
	int getPort();

	/**
	 * This method should return true if a service
	 * will eventually bind to a port. User can then
	 * do a sleep while waiting {@link #getPort()} to
	 * return the actual port number.
	 *
	 * @return True if this service will provide a port
	 */
	boolean hasPort();

	/**
	 * Get a hostname where service is running. This method
	 * should return null if service is unknown.
	 *
	 * @return Hostname of the server or null if unknown.
	 */
	String getHost();


}
