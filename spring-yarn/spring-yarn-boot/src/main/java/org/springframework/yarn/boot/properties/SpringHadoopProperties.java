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
package org.springframework.yarn.boot.properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

/**
 * Spring Boot {@link ConfigurationProperties} for <em>spring.hadoop</em>.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(value = "spring.hadoop")
public class SpringHadoopProperties {

	private final static Log log = LogFactory.getLog(SpringHadoopProperties.class);

	private String fsUri;
	private String resourceManagerHost;
	private String resourceManagerSchedulerHost;
	private Integer resourceManagerPort = 8032;
	private Integer resourceManagerSchedulerPort = 8030;

	@Autowired
	private SpringYarnEnvProperties syep;

	public String getFsUri() {
		if (log.isDebugEnabled()) {
			log.debug("syp fsUri=[" + fsUri + "]");
			log.debug("syep fsUri=[" + syep.getFs() + "]");
		}
		return syep.getFs() != null ? syep.getFs() : fsUri;
	}

	public void setFsUri(String fsUri) {
		this.fsUri = fsUri;
	}

	public String getResourceManagerAddress() {
		return syep.getRm() != null ? syep.getRm() : resourceManagerHost + ":" + getResourceManagerPort();
	}

	public void setResourceManagerAddress(String resourceManagerAddress) {
		String[] split = StringUtils.split(resourceManagerAddress, ":");
		if (split != null && split.length == 2) {
			try {
				resourceManagerPort = Integer.parseInt(split[1]);
				resourceManagerHost = split[0];
			} catch (Exception e) {
			}
		}
	}

	public String getResourceManagerSchedulerAddress() {
		return syep.getScheduler() != null ? syep.getScheduler() : resourceManagerSchedulerHost + ":" + getResourceManagerSchedulerPort();
	}

	public void setResourceManagerSchedulerAddress(String resourceManagerSchedulerAddress) {
		String[] split = StringUtils.split(resourceManagerSchedulerAddress, ":");
		if (split != null && split.length == 2) {
			try {
				resourceManagerSchedulerPort = Integer.parseInt(split[1]);
				resourceManagerSchedulerHost = split[0];
			} catch (Exception e) {
			}
		}
	}

	public String getResourceManagerHost() {
		return resourceManagerHost;
	}

	public void setResourceManagerHost(String resourceManagerHost) {
		this.resourceManagerHost = resourceManagerHost;
		this.resourceManagerSchedulerHost = resourceManagerHost;
	}

	public Integer getResourceManagerPort() {
		return resourceManagerPort;
	}

	public void setResourceManagerPort(Integer resourceManagerPort) {
		this.resourceManagerPort = resourceManagerPort;
	}

	public Integer getResourceManagerSchedulerPort() {
		return resourceManagerSchedulerPort;
	}

	public void setResourceManagerSchedulerPort(Integer resourceManagerSchedulerPort) {
		this.resourceManagerSchedulerPort = resourceManagerSchedulerPort;
	}

}
