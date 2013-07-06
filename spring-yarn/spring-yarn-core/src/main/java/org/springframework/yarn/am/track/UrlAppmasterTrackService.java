/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.am.track;

import org.springframework.yarn.am.AppmasterTrackService;

/**
 * Generic {@link AppmasterTrackService} implementation
 * whose only purpose is to expose the given url. This is
 * most useful if existing service already exists and its
 * url needs to be exposed to Application Master.
 * 
 * @author Janne Valkealahti
 *
 */
public class UrlAppmasterTrackService implements AppmasterTrackService {

	private String url;

	/**
	 * Instantiate with a given url.
	 * 
	 * @param url the url
	 */
	public UrlAppmasterTrackService(String url) {
		this.url = url;
	}

	@Override
	public String getTrackUrl() {
		return url;
	}

	/**
	 * Sets the url.
	 * 
	 * @param url the url
	 */
	public void setUrl(String url) {
		this.url = url;
	}

}
