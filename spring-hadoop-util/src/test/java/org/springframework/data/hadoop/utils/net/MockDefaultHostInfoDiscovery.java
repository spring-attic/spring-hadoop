package org.springframework.data.hadoop.utils.net;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.List;
import org.springframework.data.hadoop.util.net.DefaultHostInfoDiscovery;

public class MockDefaultHostInfoDiscovery extends DefaultHostInfoDiscovery {

	List<NetworkInterface> interfaces;

	public MockDefaultHostInfoDiscovery(List<NetworkInterface> interfaces) {
		this.interfaces = interfaces;
	}

	@Override
	protected List<NetworkInterface> getAllAvailableInterfaces() throws SocketException {
		return interfaces;
	}

}