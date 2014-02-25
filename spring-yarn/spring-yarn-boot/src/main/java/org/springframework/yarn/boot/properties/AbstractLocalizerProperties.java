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

public abstract class AbstractLocalizerProperties {

	private List<String> patterns;
	private String zipPattern;
	private List<String> propertiesNames;
	private List<String> propertiesSuffixes;

	public List<String> getPatterns() {
		return patterns;
	}

	public void setPatterns(List<String> patterns) {
		this.patterns = patterns;
	}

	public String getZipPattern() {
		return zipPattern;
	}

	public void setZipPattern(String zipPattern) {
		this.zipPattern = zipPattern;
	}

	public List<String> getPropertiesNames() {
		return propertiesNames;
	}

	public void setPropertiesNames(List<String> propertiesNames) {
		this.propertiesNames = propertiesNames;
	}

	public List<String> getPropertiesSuffixes() {
		return propertiesSuffixes;
	}

	public void setPropertiesSuffixes(List<String> propertiesSuffixes) {
		this.propertiesSuffixes = propertiesSuffixes;
	}

}
