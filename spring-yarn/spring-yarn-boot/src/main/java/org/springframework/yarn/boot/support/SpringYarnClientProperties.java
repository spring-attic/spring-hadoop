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
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot {@link ConfigurationProperties} for
 * <code>spring.yarn.client</code>.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(name = "spring.yarn.client")
public class SpringYarnClientProperties {

	private String appmasterFile;
	private List<String> files;
	private String masterRunner;
	private List<String> classpath;
	private List<String> options;
	private Map<String, String> arguments;
	private Integer priority;
	private String queue;
	private String memory;
	private Integer virtualCores;
	private Map<String, byte[]> rawFileContents;

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

	public String getMasterRunner() {
		return masterRunner;
	}

	public void setMasterRunner(String masterRunner) {
		this.masterRunner = masterRunner;
	}

	public List<String> getClasspath() {
		return classpath;
	}

	public void setClasspath(List<String> classpath) {
		this.classpath = classpath;
	}

	public List<String> getOptions() {
		return options;
	}

	public void setOptions(List<String> options) {
		this.options = options;
	}

	public Map<String, String> getArguments() {
		return arguments;
	}

	public void setArguments(Map<String, String> arguments) {
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

	public Map<String, byte[]> getRawFileContents() {
		return rawFileContents;
	}

	/**
	 * Sets the raw file contents. This configuration property is more or less
	 * used internally to transfer small amount of raw file contents from a one
	 * executing Spring Application into a new application instance.
	 *
	 * @param rawFileContents
	 *            the raw file contents
	 */
	public void setRawFileContents(Map<String, byte[]> rawFileContents) {
		// TODO: consider pulling this out into a new 'internal' config prop
		// class
		this.rawFileContents = rawFileContents;
	}

}
