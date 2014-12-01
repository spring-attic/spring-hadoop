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
package org.springframework.yarn.am.container;

/**
 * Domain class for keeping container register information together.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerRegisterInfo {

	private String trackUrl;

	/**
	 * Instantiates a new container register info.
	 */
	public ContainerRegisterInfo() {
	}

	/**
	 * Instantiates a new container register info.
	 *
	 * @param trackUrl the track url
	 */
	public ContainerRegisterInfo(String trackUrl) {
		this.trackUrl = trackUrl;
	}

	/**
	 * Gets the track url.
	 *
	 * @return the track url
	 */
	public String getTrackUrl() {
		return trackUrl;
	}

	/**
	 * Sets the track url.
	 *
	 * @param trackUrl the new track url
	 */
	public void setTrackUrl(String trackUrl) {
		this.trackUrl = trackUrl;
	}


}
