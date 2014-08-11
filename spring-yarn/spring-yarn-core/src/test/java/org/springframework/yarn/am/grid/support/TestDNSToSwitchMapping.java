package org.springframework.yarn.am.grid.support;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.net.DNSToSwitchMapping;

public class TestDNSToSwitchMapping implements DNSToSwitchMapping {

	@Override
	public List<String> resolve(List<String> names) {
		List<String> racks = new ArrayList<String>();
		for (String name : names) {
			if (name.startsWith("host1")) {
				racks.add("/rack1");
			} else if (name.startsWith("host2")) {
				racks.add("/rack2");
			} else if (name.startsWith("host3")) {
				racks.add("/rack3");
			}
		}
		return racks;
	}

	@Override
	public void reloadCachedMappings() {
	}

	public void reloadCachedMappings(List<String> names) {
		// this method added in hadoop 2.4, so keep it here
		// and don't add override for compile not to fail with 2.2
	}

}
