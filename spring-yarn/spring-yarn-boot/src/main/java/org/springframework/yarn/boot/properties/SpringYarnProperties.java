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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot {@link ConfigurationProperties} for <em>spring.yarn</em>.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(value = "spring.yarn")
public class SpringYarnProperties {

	private String applicationDir;
	private String applicationBaseDir;
	private String applicationVersion;
	private String stagingDir;
	private String appName;
	private String appType;
	private String siteYarnAppClasspath;
	private String siteMapreduceAppClasspath;

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

	public String getApplicationVersion() {
		return applicationVersion;
	}

	public void setApplicationVersion(String applicationVersion) {
		this.applicationVersion = applicationVersion;
	}

	public String getStagingDir() {
		return stagingDir;
	}

	public void setStagingDir(String stagingDir) {
		this.stagingDir = stagingDir;
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

	public String getSiteYarnAppClasspath() {
		return siteYarnAppClasspath;
	}

	public void setSiteYarnAppClasspath(String siteYarnAppClasspath) {
		this.siteYarnAppClasspath = siteYarnAppClasspath;
	}

	public String getSiteMapreduceAppClasspath() {
		return siteMapreduceAppClasspath;
	}

	public void setSiteMapreduceAppClasspath(String siteMapreduceAppClasspath) {
		this.siteMapreduceAppClasspath = siteMapreduceAppClasspath;
	}

}
