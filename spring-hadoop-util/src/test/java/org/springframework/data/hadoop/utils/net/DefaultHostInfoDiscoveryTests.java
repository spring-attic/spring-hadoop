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
package org.springframework.data.hadoop.utils.net;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.data.hadoop.util.net.HostInfoDiscovery.HostInfo;

@RunWith(PowerMockRunner.class)
@PrepareForTest({NetworkInterface.class, InterfaceAddress.class, MockDefaultHostInfoDiscovery.class})
public class DefaultHostInfoDiscoveryTests {

	private static final byte NET_PREFIX = (byte) 24;
	private static final byte[] IPV4_ADDRESS1 = new byte[] { (byte) 192, (byte) 168, 1, 1 };
	private static final byte[] IPV4_ADDRESS2 = new byte[] { 10, 10, 10, 10 };
	private static final byte[] IPV4_ADDRESS3 = new byte[] { (byte) 192, (byte) 168, (byte) 128, 1 };
	private static final byte[] IPV6_ADDRESS1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
	private static final byte[] IPV4_LOCALHOST = new byte[] { 127, 0, 0, 1 };

	@Test
	public void testOnlyLoopbackItDisabled() throws Exception {
		NetworkInterface nic0 = mock(NetworkInterface.class);
		InterfaceAddress addresses0[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_LOCALHOST)
		};
		when(nic0.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses0));
		when(nic0.isLoopback()).thenReturn(true);
		when(nic0.isPointToPoint()).thenReturn(false);
		when(nic0.getName()).thenReturn("lo");

		MockDefaultHostInfoDiscovery discovery = new MockDefaultHostInfoDiscovery(Arrays.asList(nic0));
		HostInfo hostInfo = discovery.getHostInfo();
		assertThat(hostInfo, nullValue());
	}

	@Test
	public void testOnlyLoopbackItEnabled() throws Exception {
		NetworkInterface nic0 = mock(NetworkInterface.class);
		InterfaceAddress addresses0[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_LOCALHOST)
		};
		when(nic0.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses0));
		when(nic0.isLoopback()).thenReturn(true);
		when(nic0.isPointToPoint()).thenReturn(false);
		when(nic0.getName()).thenReturn("lo");

		MockDefaultHostInfoDiscovery discovery = new MockDefaultHostInfoDiscovery(Arrays.asList(nic0));
		discovery.setLoopback(true);
		HostInfo hostInfo = discovery.getHostInfo();
		assertThat(hostInfo, notNullValue());
		assertThat(hostInfo.getAddress(), is("127.0.0.1"));
		assertThat(hostInfo.getHostname(), is("localhost"));
	}

	@Test
	public void testSingleEthSiteAddress() throws Exception {
		NetworkInterface nic0 = mock(NetworkInterface.class);
		InterfaceAddress addresses0[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_LOCALHOST)
		};
		when(nic0.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses0));
		when(nic0.isLoopback()).thenReturn(true);
		when(nic0.isPointToPoint()).thenReturn(false);
		when(nic0.getName()).thenReturn("lo");

		NetworkInterface nic1 = mock(NetworkInterface.class);
		InterfaceAddress addresses1[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS1),
				mockInterfaceAddress(IPV6_ADDRESS1)
		};
		when(nic1.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses1));
		when(nic1.isPointToPoint()).thenReturn(false);
		when(nic1.getName()).thenReturn("eth0");

		MockDefaultHostInfoDiscovery discovery = new MockDefaultHostInfoDiscovery(Arrays.asList(nic0, nic1));
		HostInfo hostInfo = discovery.getHostInfo();
		assertThat(hostInfo, notNullValue());
		assertThat(hostInfo.getAddress(), is("192.168.1.1"));
		assertThat(hostInfo.getHostname(), is("fakeHostName"));
	}

	@Test
	public void testTwoNicsPreferEth() throws Exception {
		NetworkInterface nic0 = mock(NetworkInterface.class);
		InterfaceAddress addresses0[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_LOCALHOST)
		};
		when(nic0.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses0));
		when(nic0.isLoopback()).thenReturn(true);
		when(nic0.isPointToPoint()).thenReturn(false);
		when(nic0.getName()).thenReturn("lo");

		NetworkInterface nic1 = mock(NetworkInterface.class);
		InterfaceAddress addresses1[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS1),
				mockInterfaceAddress(IPV6_ADDRESS1)
		};
		when(nic1.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses1));
		when(nic1.isPointToPoint()).thenReturn(false);
		when(nic1.getName()).thenReturn("eth0");

		NetworkInterface nic2 = mock(NetworkInterface.class);
		InterfaceAddress addresses2[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS2)
		};
		when(nic2.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses2));
		when(nic2.isPointToPoint()).thenReturn(false);
		when(nic2.getName()).thenReturn("foo0");

		MockDefaultHostInfoDiscovery discovery = new MockDefaultHostInfoDiscovery(Arrays.asList(nic0, nic1, nic2));
		HostInfo hostInfo = discovery.getHostInfo();
		assertThat(hostInfo, notNullValue());
		assertThat(hostInfo.getAddress(), is("192.168.1.1"));
		assertThat(hostInfo.getHostname(), is("fakeHostName"));
	}

	@Test
	public void testTwoNicsPreferEn() throws Exception {
		NetworkInterface nic0 = mock(NetworkInterface.class);
		InterfaceAddress addresses0[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_LOCALHOST)
		};
		when(nic0.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses0));
		when(nic0.isLoopback()).thenReturn(true);
		when(nic0.isPointToPoint()).thenReturn(false);
		when(nic0.getName()).thenReturn("lo");

		NetworkInterface nic1 = mock(NetworkInterface.class);
		InterfaceAddress addresses1[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS1),
				mockInterfaceAddress(IPV6_ADDRESS1)
		};
		when(nic1.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses1));
		when(nic1.isPointToPoint()).thenReturn(false);
		when(nic1.getName()).thenReturn("en0");

		NetworkInterface nic2 = mock(NetworkInterface.class);
		InterfaceAddress addresses2[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS2)
		};
		when(nic2.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses2));
		when(nic2.isPointToPoint()).thenReturn(false);
		when(nic2.getName()).thenReturn("foo0");

		MockDefaultHostInfoDiscovery discovery = new MockDefaultHostInfoDiscovery(Arrays.asList(nic0, nic1, nic2));
		HostInfo hostInfo = discovery.getHostInfo();
		assertThat(hostInfo, notNullValue());
		assertThat(hostInfo.getAddress(), is("192.168.1.1"));
		assertThat(hostInfo.getHostname(), is("fakeHostName"));
	}

	@Test
	public void testMatchInterface() throws Exception {
		NetworkInterface nic0 = mock(NetworkInterface.class);
		InterfaceAddress addresses0[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_LOCALHOST)
		};
		when(nic0.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses0));
		when(nic0.isLoopback()).thenReturn(true);
		when(nic0.isPointToPoint()).thenReturn(false);
		when(nic0.getName()).thenReturn("lo");

		NetworkInterface nic1 = mock(NetworkInterface.class);
		InterfaceAddress addresses1[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS1)
		};
		when(nic1.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses1));
		when(nic1.isPointToPoint()).thenReturn(false);
		when(nic1.getName()).thenReturn("en0");

		NetworkInterface nic2 = mock(NetworkInterface.class);
		InterfaceAddress addresses2[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS2)
		};
		when(nic2.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses2));
		when(nic2.isPointToPoint()).thenReturn(false);
		when(nic2.getName()).thenReturn("foo0");

		MockDefaultHostInfoDiscovery discovery = new MockDefaultHostInfoDiscovery(Arrays.asList(nic0, nic1, nic2));
		discovery.setMatchInterface("foo\\d*");
		HostInfo hostInfo = discovery.getHostInfo();
		assertThat(hostInfo, notNullValue());
		assertThat(hostInfo.getAddress(), is("10.10.10.10"));
		assertThat(hostInfo.getHostname(), is("fakeHostName"));
	}

	@Test
	public void testMatchCidr1() throws Exception {
		NetworkInterface nic0 = mock(NetworkInterface.class);
		InterfaceAddress addresses0[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_LOCALHOST)
		};
		when(nic0.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses0));
		when(nic0.isLoopback()).thenReturn(true);
		when(nic0.isPointToPoint()).thenReturn(false);
		when(nic0.getName()).thenReturn("lo");

		NetworkInterface nic1 = mock(NetworkInterface.class);
		InterfaceAddress addresses1[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS1)
		};
		when(nic1.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses1));
		when(nic1.isPointToPoint()).thenReturn(false);
		when(nic1.getName()).thenReturn("eth0");

		NetworkInterface nic2 = mock(NetworkInterface.class);
		InterfaceAddress addresses2[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS2)
		};
		when(nic2.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses2));
		when(nic2.isPointToPoint()).thenReturn(false);
		when(nic2.getName()).thenReturn("eth1");

		NetworkInterface nic3 = mock(NetworkInterface.class);
		InterfaceAddress addresses3[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS3)
		};
		when(nic3.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses3));
		when(nic3.isPointToPoint()).thenReturn(false);
		when(nic3.getName()).thenReturn("eth2");

		MockDefaultHostInfoDiscovery discovery = new MockDefaultHostInfoDiscovery(Arrays.asList(nic0, nic1, nic2, nic3));
		discovery.setMatchIpv4("192.168.1.1/24");
		HostInfo hostInfo = discovery.getHostInfo();
		assertThat(hostInfo, notNullValue());
		assertThat(hostInfo.getAddress(), is("192.168.1.1"));
		assertThat(hostInfo.getHostname(), is("fakeHostName"));
	}

	@Test
	public void testMatchCidr2() throws Exception {
		NetworkInterface nic0 = mock(NetworkInterface.class);
		InterfaceAddress addresses0[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_LOCALHOST)
		};
		when(nic0.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses0));
		when(nic0.isLoopback()).thenReturn(true);
		when(nic0.isPointToPoint()).thenReturn(false);
		when(nic0.getName()).thenReturn("lo");

		NetworkInterface nic1 = mock(NetworkInterface.class);
		InterfaceAddress addresses1[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS1)
		};
		when(nic1.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses1));
		when(nic1.isPointToPoint()).thenReturn(false);
		when(nic1.getName()).thenReturn("eth0");

		NetworkInterface nic2 = mock(NetworkInterface.class);
		InterfaceAddress addresses2[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS2)
		};
		when(nic2.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses2));
		when(nic2.isPointToPoint()).thenReturn(false);
		when(nic2.getName()).thenReturn("eth1");

		NetworkInterface nic3 = mock(NetworkInterface.class);
		InterfaceAddress addresses3[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS3)
		};
		when(nic3.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses3));
		when(nic3.isPointToPoint()).thenReturn(false);
		when(nic3.getName()).thenReturn("eth2");

		MockDefaultHostInfoDiscovery discovery = new MockDefaultHostInfoDiscovery(Arrays.asList(nic0, nic1, nic2, nic3));
		discovery.setMatchIpv4("192.168.128.1/22");
		HostInfo hostInfo = discovery.getHostInfo();
		assertThat(hostInfo, notNullValue());
		assertThat(hostInfo.getAddress(), is("192.168.128.1"));
		assertThat(hostInfo.getHostname(), is("fakeHostName"));
	}

	@Test
	public void testPreferNicIndex() throws Exception {
		NetworkInterface nic0 = mock(NetworkInterface.class);
		InterfaceAddress addresses0[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_LOCALHOST)
		};
		when(nic0.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses0));
		when(nic0.isLoopback()).thenReturn(true);
		when(nic0.isPointToPoint()).thenReturn(false);
		when(nic0.getName()).thenReturn("lo");

		NetworkInterface nic1 = mock(NetworkInterface.class);
		InterfaceAddress addresses1[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS1)
		};
		when(nic1.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses1));
		when(nic1.isPointToPoint()).thenReturn(false);
		when(nic1.getName()).thenReturn("eth1");
		when(nic1.getIndex()).thenReturn(1);

		NetworkInterface nic2 = mock(NetworkInterface.class);
		InterfaceAddress addresses2[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS2)
		};
		when(nic2.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses2));
		when(nic2.isPointToPoint()).thenReturn(false);
		when(nic2.getName()).thenReturn("eth0");
		when(nic2.getIndex()).thenReturn(0);

		NetworkInterface nic3 = mock(NetworkInterface.class);
		InterfaceAddress addresses3[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS3)
		};
		when(nic3.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses3));
		when(nic3.isPointToPoint()).thenReturn(false);
		when(nic3.getName()).thenReturn("eth2");
		when(nic3.getIndex()).thenReturn(2);

		MockDefaultHostInfoDiscovery discovery = new MockDefaultHostInfoDiscovery(Arrays.asList(nic0, nic1, nic2, nic3));
		HostInfo hostInfo = discovery.getHostInfo();
		assertThat(hostInfo, notNullValue());
		assertThat(hostInfo.getAddress(), is("10.10.10.10"));
		assertThat(hostInfo.getHostname(), is("fakeHostName"));
	}

	@Test
	public void testComplexDiscoveryOptions() throws Exception {
		NetworkInterface nic0 = mock(NetworkInterface.class);
		InterfaceAddress addresses0[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_LOCALHOST)
		};
		when(nic0.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses0));
		when(nic0.isLoopback()).thenReturn(true);
		when(nic0.isPointToPoint()).thenReturn(false);
		when(nic0.getName()).thenReturn("lo");

		NetworkInterface nic1 = mock(NetworkInterface.class);
		InterfaceAddress addresses1[] = new InterfaceAddress[]{
				mockInterfaceAddress(IPV4_ADDRESS1),
				mockInterfaceAddress(IPV6_ADDRESS1)
		};
		when(nic1.getInterfaceAddresses()).thenReturn(Arrays.asList(addresses1));
		when(nic1.isPointToPoint()).thenReturn(false);
		when(nic1.getName()).thenReturn("eth0");

		MockDefaultHostInfoDiscovery discovery = new MockDefaultHostInfoDiscovery(Arrays.asList(nic0, nic1));
		discovery.setMatchInterface("eth\\d*");
		discovery.setMatchIpv4("192.168.1.1/24");
		HostInfo hostInfo = discovery.getHostInfo();
		assertThat(hostInfo, notNullValue());
		assertThat(hostInfo.getAddress(), is("192.168.1.1"));
		assertThat(hostInfo.getHostname(), is("fakeHostName"));
	}

	private InterfaceAddress mockInterfaceAddress(final byte[] netAddress) {
		final InterfaceAddress intAddress = mockInterfaceAddress(netAddress, NET_PREFIX);
		if (netAddress.equals(IPV4_LOCALHOST)) {
			InetAddress address = mock(InetAddress.class);
			when(address.getAddress()).thenReturn(IPV4_LOCALHOST);
			when(address.isLoopbackAddress()).thenReturn(true);
			when(address.isAnyLocalAddress()).thenReturn(false);
			when(address.getHostAddress()).thenReturn("127.0.0.1");
			when(address.getHostName()).thenReturn("localhost");
			when(intAddress.getAddress()).thenReturn(address);
		} else if (netAddress.equals(IPV4_ADDRESS1)) {
			InetAddress address = mock(InetAddress.class);
			when(address.getAddress()).thenReturn(IPV4_ADDRESS1);
			when(address.isLoopbackAddress()).thenReturn(false);
			when(address.isAnyLocalAddress()).thenReturn(false);
			when(address.getHostAddress()).thenReturn("192.168.1.1");
			when(address.getHostName()).thenReturn("fakeHostName");
			when(intAddress.getAddress()).thenReturn(address);
		} else if (netAddress.equals(IPV4_ADDRESS2)) {
			InetAddress address = mock(InetAddress.class);
			when(address.getAddress()).thenReturn(IPV4_ADDRESS2);
			when(address.isLoopbackAddress()).thenReturn(false);
			when(address.getHostAddress()).thenReturn("10.10.10.10");
			when(address.getHostName()).thenReturn("fakeHostName");
			when(intAddress.getAddress()).thenReturn(address);
		} else if (netAddress.equals(IPV4_ADDRESS3)) {
			InetAddress address = mock(InetAddress.class);
			when(address.getAddress()).thenReturn(IPV4_ADDRESS3);
			when(address.isLoopbackAddress()).thenReturn(false);
			when(address.isAnyLocalAddress()).thenReturn(false);
			when(address.getHostAddress()).thenReturn("192.168.128.1");
			when(address.getHostName()).thenReturn("fakeHostName");
			when(intAddress.getAddress()).thenReturn(address);
		}
		return intAddress;
	}

	private InterfaceAddress mockInterfaceAddress(final byte[] netAddress, final int netPrefix) {
		InetAddress address = mock(InetAddress.class);
		when(address.getAddress()).thenReturn(netAddress);

		InterfaceAddress adapter = mock(InterfaceAddress.class);
		when(adapter.getAddress()).thenReturn(address);
		when(adapter.getNetworkPrefixLength()).thenReturn((short) netPrefix);
		return adapter;
	}
}
