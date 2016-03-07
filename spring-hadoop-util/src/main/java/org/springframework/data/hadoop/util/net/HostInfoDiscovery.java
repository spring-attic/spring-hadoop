/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.hadoop.util.net;

/**
 * Interface used to discover a {@link HostInfo} from a system.
 *
 * @author Janne Valkealahti
 *
 */
public interface HostInfoDiscovery {

	/**
	 * Gets the host info.
	 *
	 * @return the host info
	 */
	HostInfo getHostInfo();

	/**
	 * Class wrapping host information.
	 */
	public static final class HostInfo {
		private final String address;
		private final String hostname;

		/**
		 * Instantiates a new host info.
		 *
		 * @param address the address
		 * @param hostname the hostname
		 */
		public HostInfo(String address, String hostname) {
			super();
			this.address = address;
			this.hostname = hostname;
		}

		/**
		 * Gets the ip address as represented in string.
		 *
		 * @return the ip address
		 */
		public String getAddress() {
			return address;
		}

		/**
		 * Gets the hostname.
		 *
		 * @return the hostname
		 */
		public String getHostname() {
			return hostname;
		}
	}

}
