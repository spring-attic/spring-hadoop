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
package org.springframework.yarn.boot.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot {@link ConfigurationProperties} for <code>spring.yarn</code>.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(name = "spring.yarn")
public class SpringYarnProperties {

	private final static Log log = LogFactory.getLog(SpringYarnProperties.class);

	private String applicationDir;
	private String applicationsBaseDir;
	private String fsUri;
	private String rmAddress;
	private String schedulerAddress;
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

	public String getApplicationsBaseDir() {
		return applicationsBaseDir;
	}

	public void setApplicationsBaseDir(String applicationsBaseDir) {
		this.applicationsBaseDir = applicationsBaseDir;
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

	public String getRmAddress() {
		if (log.isDebugEnabled()) {
			log.debug("syp rmAddress=[" + rmAddress + "]");
			log.debug("syep rmAddress=[" + syep.getRm() + "]");
		}
		return syep.getRm() != null ? syep.getRm() : rmAddress;
	}

	public void setRmAddress(String rmAddress) {
		this.rmAddress = rmAddress;
	}

	public String getSchedulerAddress() {
		return syep.getScheduler() != null ? syep.getScheduler() : schedulerAddress;
	}

	public void setSchedulerAddress(String schedulerAddress) {
		this.schedulerAddress = schedulerAddress;
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
