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

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot {@link ConfigurationProperties} for <code>spring.yarn.client</code>.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(name = "spring.yarn.client")
public class SpringYarnClientProperties {

	private String appmasterFile;
	private List<String> files;
	private String runnerClazz;
	private List<String> classpath;
	private List<String> arguments;
	private Integer priority;
	private String queue;
	private String memory;
	private Integer virtualCores;

	public String getAppmasterFile() {
		return appmasterFile;
	}

	public void setAppmasterFile(String appmasterFile) {
		this.appmasterFile = appmasterFile;
	}

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public String getRunnerClazz() {
		return runnerClazz;
	}

	public void setRunnerClazz(String runnerClazz) {
		this.runnerClazz = runnerClazz;
	}

	public List<String> getClasspath() {
		return classpath;
	}

	public void setClasspath(List<String> classpath) {
		this.classpath = classpath;
	}

	public List<String> getArguments() {
		return arguments;
	}

	public void setArguments(List<String> arguments) {
		this.arguments = arguments;
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

	public String getMemory() {
		return memory;
	}

	public void setMemory(String memory) {
		this.memory = memory;
	}

	public Integer getVirtualCores() {
		return virtualCores;
	}

	public void setVirtualCores(Integer virtualCores) {
		this.virtualCores = virtualCores;
	}

}
