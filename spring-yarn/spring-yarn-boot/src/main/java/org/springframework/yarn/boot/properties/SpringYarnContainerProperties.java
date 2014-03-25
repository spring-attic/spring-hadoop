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
 * Spring Boot {@link ConfigurationProperties} for
 * <em>spring.yarn.container</em>.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(value = "spring.yarn.container")
public class SpringYarnContainerProperties {

	private String containerClass;
	private boolean keepContextAlive = true;

	public String getContainerClass() {
		return containerClass;
	}

	public void setContainerClass(String containerClass) {
		this.containerClass = containerClass;
	}

	public boolean isKeepContextAlive() {
		return keepContextAlive;
	}

	public void setKeepContextAlive(boolean keepContextAlive) {
		this.keepContextAlive = keepContextAlive;
	}

}
