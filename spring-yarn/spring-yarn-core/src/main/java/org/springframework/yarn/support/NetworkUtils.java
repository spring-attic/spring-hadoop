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
package org.springframework.yarn.support;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * Various network utilities.
 *
 * @author Janne Valkealahti
 *
 */
public class NetworkUtils {

	/**
	 * Gets the main network address.
	 *
	 * @return network address, null if not found
	 */
	public static String getDefaultAddress() {
		Enumeration<NetworkInterface> nets;
		try {
			nets = NetworkInterface.getNetworkInterfaces();
		} catch (SocketException e) {
			return null;
		}
		NetworkInterface netinf;
		while (nets.hasMoreElements()) {
			netinf = nets.nextElement();

			Enumeration<InetAddress> addresses = netinf.getInetAddresses();

			while (addresses.hasMoreElements()) {
				InetAddress address = addresses.nextElement();
				if (!address.isAnyLocalAddress() && !address.isMulticastAddress() && !(address instanceof Inet6Address)) {
					return address.getHostAddress();
				}
			}
		}
		return null;
	}

}
