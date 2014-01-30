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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot {@link ConfigurationProperties} used in
 * {@code SpringYarnClientProperties}, {@code SpringYarnAppmasterProperties} and
 * {@code SpringYarnProperties} to resolve supported environment variables.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties
public class SpringYarnEnvProperties {

	private String fs;
	private String rm;
	private String scheduler;

	/**
	 * Gets the value of environment variable <code>SHDP_HD_FS</code>.
	 *
	 * @return the value of <code>SHDP_HD_FS</code> or <code>null</code>
	 */
	public String getFs() {
		return fs;
	}

	/**
	 * Sets the value from an environment variable <code>SHDP_HD_FS</code>.
	 */
	public void setSHDP_HD_FS(String fs) {
		this.fs = fs;
	}

	/**
	 * Gets the value of environment variable <code>SHDP_HD_RM</code>.
	 *
	 * @return the value of <code>SHDP_HD_RM</code> or <code>null</code>
	 */
	public String getRm() {
		return rm;
	}

	/**
	 * Sets the value from an environment variable <code>SHDP_HD_RM</code>.
	 */
	public void setSHDP_HD_RM(String rm) {
		this.rm = rm;
	}

	/**
	 * Gets the value of environment variable <code>SHDP_HD_SCHEDULER</code>.
	 *
	 * @return the value of <code>SHDP_HD_SCHEDULER</code> or <code>null</code>
	 */
	public String getScheduler() {
		return scheduler;
	}

	/**
	 * Sets the value from an environment variable
	 * <code>SHDP_HD_SCHEDULER</code>.
	 */
	public void setSHDP_HD_SCHEDULER(String scheduler) {
		this.scheduler = scheduler;
	}

}
