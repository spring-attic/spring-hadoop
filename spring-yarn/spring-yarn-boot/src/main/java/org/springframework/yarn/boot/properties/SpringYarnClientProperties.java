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

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot {@link ConfigurationProperties} for
 * <em>spring.yarn.client</em>.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(name = "spring.yarn.client")
public class SpringYarnClientProperties {

	private List<String> files;
	private Integer priority;
	private String queue;

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public Integer getPriority() {
		return priority;
	}

	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

}
