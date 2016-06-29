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

import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;

import org.springframework.data.hadoop.util.net.HostInfoDiscovery;
import org.springframework.util.StringUtils;

/**
 * Default implementation of {@link HostInfoDiscovery}.
 * <p>
 * Discovery logic for finding ip address is:
 * <ul>
 * <li>all possible network interfaces are requested
 * <li>for interfaces, filter out point to point if not enabled
 * <li>for interfaces, filter out loopback if not enabled
 * <li>for interfaces, explicit regex patter match is done if pattern is set
 * <li>interfaces are sort by preferred name prefixes, on default "eth" and "en" are sort first
 * <li>interfaces are sorted by their indexes assuming eth0 should be picked over eth1
 * <li>interfaces are checked by their list of ip addresses
 * <li>only ipv4 ip's are taken
 * <li>cidr notation to match if from a network/mask is taken in defined
 * <li>what is left, first found ip is taken
 * </ul>
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultHostInfoDiscovery implements HostInfoDiscovery {

	private String matchIpv4;
	private String matchInterface;
	private List<String> preferInterface = Arrays.asList("eth", "en");
	private boolean pointToPoint = false;
	private boolean loopback = false;

	@Override
	public HostInfo getHostInfo() {
		List<NetworkInterface> interfaces;
		try {
			interfaces = getAllAvailableInterfaces();
		} catch (SocketException e) {
			return null;
		}

		// pre filter candidates
		interfaces = filterInterfaces(interfaces);

		// sort to prepare getting first match
		interfaces = sortInterfaces(interfaces);

		for (NetworkInterface nic : interfaces) {
			List<InetAddress> addresses = new ArrayList<InetAddress>();
			for (InterfaceAddress interfaceAddress : nic.getInterfaceAddresses()) {
				addresses.add(interfaceAddress.getAddress());
			}
			addresses = filterAddresses(addresses);
			if (!addresses.isEmpty()) {
				InetAddress address = addresses.get(0);
				return new HostInfo(address.getHostAddress(), address.getHostName());
			}
		}
		return null;
	}

	/**
	 * Sets the match ipv4. Used to match ip address from
	 * a network using a cidr notation. For example, "192.168.0.1/24"
	 * matches range "192.168.0.1-192.168.0.254", "192.168.0.1/16"
	 * matches ranre "192.168.0.1-192.168.255.254" and
	 * "10.0.0.1/8" matches range "10.0.0.1-10.255.255.254"
	 *
	 * @param matchIpv4 the new match ipv4
	 */
	public void setMatchIpv4(String matchIpv4) {
		this.matchIpv4 = matchIpv4;
	}

	/**
	 * Use interface as a candidate if its name is matching with a
	 * given pattern. Default value is is empty.
	 *
	 * @param matchInterface the new match interface regex patter
	 * @see NetworkInterface#getName()
	 */
	public void setMatchInterface(String matchInterface) {
		this.matchInterface = matchInterface;
	}

	/**
	 * Sets the preferred interfaces. Sort interfaces in such
	 * order that interface names prefixed with values found from
	 * a list is considered first candidates. Defaults to "eth" and
	 * "en" which usually are the ones found from unix systems.
	 *
	 * @param preferInterface the new preferred interface list
	 */
	public void setPreferInterface(List<String> preferInterface) {
		this.preferInterface = preferInterface;
	}

	/**
	 * Sets if interfaces marked as point to point should be handled.
	 * Point to point nic is usually a vpn tunnel which may not be no
	 * use to talk to a host unless communication goes through vpn.
	 * Default value is <code>FALSE</code>.
	 *
	 * @param pointToPoint the new point to point flag
	 * @see NetworkInterface#isPointToPoint()
	 */
	public void setPointToPoint(boolean pointToPoint) {
		this.pointToPoint = pointToPoint;
	}

	/**
	 * Sets if loopback should be discovered.
	 * Default value is <code>FALSE</code>.
	 *
	 * @param loopback the new loopback flag
	 */
	public void setLoopback(boolean loopback) {
		this.loopback = loopback;
	}

	protected List<NetworkInterface> getAllAvailableInterfaces() throws SocketException {
		List<NetworkInterface> interfaces = new ArrayList<NetworkInterface>(5);
		for (Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces(); e.hasMoreElements();) {
			interfaces.add(e.nextElement());
		}
		return interfaces;
	}

	private List<InetAddress> filterAddresses(List<InetAddress> addresses) {
		List<InetAddress> filtered = new ArrayList<InetAddress>();
		for (InetAddress address : addresses) {
			// take only ipv4 addresses whose byte array length is always 4
			boolean match = address.getAddress() != null && address.getAddress().length == 4;

			// check loopback
			if (!loopback && address.isLoopbackAddress()) {
				match = false;
			}

			// match cidr if defined
			if (match && StringUtils.hasText(matchIpv4)) {
				match = matchIpv4(matchIpv4, address.getHostAddress());
			}

			if (match) {
				filtered.add(address);
			}
		}
		return filtered;
	}

	private boolean matchIpv4(String addressMatch, String address) {
		// TODO: should we fork utils from jakarta commons?
		SubnetUtils subnetUtils = new SubnetUtils(addressMatch);
		SubnetInfo info = subnetUtils.getInfo();
		return info.isInRange(address);
	}

	private List<NetworkInterface> filterInterfaces(List<NetworkInterface> interfaces) {
		List<NetworkInterface> filtered = new ArrayList<NetworkInterface>();
		for (NetworkInterface nic : interfaces) {
			boolean match = false;

			try {
				match = pointToPoint && nic.isPointToPoint();
			} catch (SocketException e) {
			}

			try {
				match = !match && loopback && nic.isLoopback();
			} catch (SocketException e) {
			}

			// last, if we didn't match anything, let all pass
			// if matchInterface is not set, otherwise do pattern
			// matching
			if (!match && !StringUtils.hasText(matchInterface)) {
				match = true;
			} else if (StringUtils.hasText(matchInterface)) {
				match = nic.getName().matches(matchInterface);
			}

			if (match) {
				filtered.add(nic);
			}
		}
		return filtered;
	}

	private List<NetworkInterface> sortInterfaces(List<NetworkInterface> interfaces) {
		Collections.sort(interfaces, new NicIndexComparator());
		Collections.sort(interfaces, new NicPreferNameComparator());
		return interfaces;
	}

	/**
	 * Comparator to sort with nic index.
	 */
	private class NicIndexComparator implements Comparator<NetworkInterface> {

		@Override
		public int compare(NetworkInterface o1, NetworkInterface o2) {
			return Integer.compare(o1.getIndex(), o2.getIndex());
		}
	}

	/**
	 * Comparator for nic names preferring a list of give prefixes order
	 * to sort those before any other.
	 */
	private class NicPreferNameComparator implements Comparator<NetworkInterface> {

		@Override
		public int compare(NetworkInterface o1, NetworkInterface o2) {
			String o1name = o1.getName();
			String o2name = o2.getName();
			if (startWithAny(preferInterface, o1name) && startWithAny(preferInterface, o2name)) {
				return 0;
			} else if (startWithAny(preferInterface, o1name) && !startWithAny(preferInterface, o2name)) {
				return -1;
			} else if (!startWithAny(preferInterface, o1name) && startWithAny(preferInterface, o2name)) {
				return 1;
			} else {
				return o1name.compareTo(o2name);
			}
		}

		private boolean startWithAny(List<String> prefixes, String name) {
			for (String prefix : prefixes) {
				if (name.startsWith(prefix)) {
					return true;
				}
			}
			return false;
		}
	}

	@Override
	public String toString() {
		return "DefaultHostInfoDiscovery [matchIpv4=" + matchIpv4 + ", matchInterface=" + matchInterface + ", preferInterface="
				+ preferInterface + ", pointToPoint=" + pointToPoint + ", loopback=" + loopback + "]";
	}

}
