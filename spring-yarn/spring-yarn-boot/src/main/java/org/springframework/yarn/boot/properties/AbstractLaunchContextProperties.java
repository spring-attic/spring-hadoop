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
import java.util.Map;

public abstract class AbstractLaunchContextProperties {

	private String archiveFile;
	private String runnerClass;
	private List<String> options;
	private Map<String, String> arguments;
	private List<String> classpath;
	private String pathSeparator;
	private boolean includeBaseDirectory = true;
	private boolean useDefaultYarnClasspath = true;
	private boolean includeSystemEnv = true;

	public String getArchiveFile() {
		return archiveFile;
	}

	public void setArchiveFile(String archiveFile) {
		this.archiveFile = archiveFile;
	}

	public String getRunnerClass() {
		return runnerClass;
	}

	public void setRunnerClass(String runnerClass) {
		this.runnerClass = runnerClass;
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

	public List<String> getClasspath() {
		return classpath;
	}

	public void setClasspath(List<String> classpath) {
		this.classpath = classpath;
	}

	public String getPathSeparator() {
		return pathSeparator;
	}

	public void setPathSeparator(String pathSeparator) {
		this.pathSeparator = pathSeparator;
	}

	public boolean isIncludeBaseDirectory() {
		return includeBaseDirectory;
	}

	public void setIncludeBaseDirectory(boolean includeBaseDirectory) {
		this.includeBaseDirectory = includeBaseDirectory;
	}

	public boolean isUseDefaultYarnClasspath() {
		return useDefaultYarnClasspath;
	}

	public void setUseDefaultYarnClasspath(boolean useDefaultYarnClasspath) {
		this.useDefaultYarnClasspath = useDefaultYarnClasspath;
	}

	public boolean isIncludeSystemEnv() {
		return includeSystemEnv;
	}

	public void setIncludeSystemEnv(boolean includeSystemEnv) {
		this.includeSystemEnv = includeSystemEnv;
	}

}
