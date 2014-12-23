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
package org.springframework.yarn.am.grid.support;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.net.DNSToSwitchMapping;

/**
 * Custom rack resolver to get expected result for rack resolving.
 * 
 * @author Janne Valkealahti
 *
 */
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
			} else {
				racks.add("/default-rack");
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
