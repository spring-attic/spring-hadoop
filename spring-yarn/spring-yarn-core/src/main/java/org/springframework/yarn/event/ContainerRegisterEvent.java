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
package org.springframework.yarn.event;

import org.apache.hadoop.yarn.api.records.Container;

/**
 * Generic event representing that {@link Container} has been registered. This
 * event is usually sent into context via boot endpoint handling registering
 * http posts from a YARN containers.
 *
 * @author Janne Valkealahti
 *
 */
@SuppressWarnings("serial")
public class ContainerRegisterEvent extends AbstractYarnEvent {

	private String containerId;

	private String trackUrl;

	/**
	 * Instantiates a new container register event.
	 *
	 * @param source the source
	 * @param containerId the container id
	 * @param trackUrl the track url
	 */
	public ContainerRegisterEvent(Object source, String containerId, String trackUrl) {
		super(source);
		this.containerId = containerId;
		this.trackUrl = trackUrl;
	}

	/**
	 * Gets the container id.
	 *
	 * @return the container id
	 */
	public String getContainerId() {
		return containerId;
	}

	/**
	 * Gets the track url.
	 *
	 * @return the track url
	 */
	public String getTrackUrl() {
		return trackUrl;
	}

	@Override
	public String toString() {
		return "ContainerRegisterEvent [containerId=" + containerId + ", trackUrl=" + trackUrl + "]";
	}

}
