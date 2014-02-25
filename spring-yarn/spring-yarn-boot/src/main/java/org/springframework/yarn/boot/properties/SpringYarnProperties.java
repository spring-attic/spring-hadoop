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

/**
 * Spring Boot {@link ConfigurationProperties} for <em>spring.yarn</em>.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(name = "spring.yarn")
public class SpringYarnProperties {

	private final static Log log = LogFactory.getLog(SpringYarnProperties.class);

	private String applicationDir;
	private String applicationBaseDir;
	private String stagingDir;
	private String fsUri;
	private String resourceManagerHost;
	private Integer resourceManagerPort = 8032;
	private Integer resourceManagerSchedulerPort = 8030;
	private String appName;
	private String appType;

	@Autowired
	private SpringYarnEnvProperties syep;

	public String getApplicationDir() {
		return applicationDir;
	}

	public void setApplicationDir(String applicationDir) {
		this.applicationDir = applicationDir;
	}

	public String getApplicationBaseDir() {
		return applicationBaseDir;
	}

	public void setApplicationBaseDir(String applicationBaseDir) {
		this.applicationBaseDir = applicationBaseDir;
	}

	public String getStagingDir() {
		return stagingDir;
	}

	public void setStagingDir(String stagingDir) {
		this.stagingDir = stagingDir;
	}

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
		return syep.getRm() != null ? syep.getRm() : getResourceManagerHost() + ":" + getResourceManagerPort();
	}

	public String getResourceManagerSchedulerAddress() {
		return syep.getScheduler() != null ? syep.getScheduler() : getResourceManagerHost() + ":" + getResourceManagerSchedulerPort();
	}

	public String getResourceManagerHost() {
		return resourceManagerHost;
	}

	public void setResourceManagerHost(String resourceManagerHost) {
		this.resourceManagerHost = resourceManagerHost;
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

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getAppType() {
		return appType;
	}

	public void setAppType(String appType) {
		this.appType = appType;
	}

}
