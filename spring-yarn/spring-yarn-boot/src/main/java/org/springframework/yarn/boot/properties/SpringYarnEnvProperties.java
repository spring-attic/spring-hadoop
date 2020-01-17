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
	private String trackUrl;
	private String containerId;

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
	 *
	 * @param fs file system URL
	 */
	public void setShdpHdFs(String fs) {
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
	 *
	 * @param rm resource manager address
	 */
	public void setShdpHdRm(String rm) {
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
	 *
	 * @param scheduler scheduler address
	 */
	public void setShdpHdScheduler(String scheduler) {
		this.scheduler = scheduler;
	}

	/**
	 * Gets the value of environment variable <code>SHDP_AMSERVICE_TRACKURL</code>.
	 *
	 * @return the value of <code>SHDP_AMSERVICE_TRACKURL</code> or <code>null</code>
	 */
	public String getTrackUrl() {
		return trackUrl;
	}

	/**
	 * Sets the value from an environment variable
	 * <code>SHDP_AMSERVICE_TRACKURL</code>.
	 *
	 * @param trackUrl track url
	 */
	public void setShdpAmserviceTrackurl(String trackUrl) {
		this.trackUrl = trackUrl;
	}

	/**
	 * Gets the value of environment variable <code>SHDP_CONTAINERID</code>.
	 *
	 * @return the value of <code>SHDP_CONTAINERID</code> or <code>null</code>
	 */
	public String getContainerId() {
		return containerId;
	}

	/**
	 * Sets the value from an environment variable
	 * <code>SHDP_CONTAINERID</code>.
	 *
	 * @param containerId container id
	 */
	public void setShdpContainerid(String containerId) {
      setContainerId(containerId);
    }

	/**
	 * TODO: Why is it needed ???
     * Sets the value from an environment variable
     * <code>CONTAINER_ID</code>.
     *
     * @param containerId container id
     */
	public void setContainerId(String containerId) {
		this.containerId = containerId;
	}

}
