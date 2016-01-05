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
package org.springframework.yarn.boot.properties;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Shared boot configuration properties for "spring.yarn.hostdiscovery".
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(value = "spring.yarn.hostdiscovery")
public class SpringYarnHostInfoDiscoveryProperties {

	private String matchIpv4;
	private String matchInterface;
	private List<String> preferInterface;
	private boolean pointToPoint = false;
	private boolean loopback = false;

	public String getMatchIpv4() {
		return matchIpv4;
	}

	public void setMatchIpv4(String matchIpv4) {
		this.matchIpv4 = matchIpv4;
	}

	public String getMatchInterface() {
		return matchInterface;
	}

	public void setMatchInterface(String matchInterface) {
		this.matchInterface = matchInterface;
	}

	public List<String> getPreferInterface() {
		return preferInterface;
	}

	public void setPreferInterface(List<String> preferInterface) {
		this.preferInterface = preferInterface;
	}

	public boolean isPointToPoint() {
		return pointToPoint;
	}

	public void setPointToPoint(boolean pointToPoint) {
		this.pointToPoint = pointToPoint;
	}

	public boolean isLoopback() {
		return loopback;
	}

	public void setLoopback(boolean loopback) {
		this.loopback = loopback;
	}

}
