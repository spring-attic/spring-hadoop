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
package org.springframework.yarn.config.annotation.configurers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerAdapter;
import org.springframework.util.StringUtils;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentBuilder;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigurer;

/**
 * {@link org.springframework.data.config.annotation.AnnotationConfigurer AnnotationConfigurer}
 * which knows how to handle configuring a classpath.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultEnvironmentClasspathConfigurer
		extends AnnotationConfigurerAdapter<Map<String, String>, YarnEnvironmentConfigurer, YarnEnvironmentBuilder>
		implements EnvironmentClasspathConfigurer {

	private boolean useDefaultYarnClasspath = true;
	private String defaultYarnAppClasspath;
	private boolean includeBaseDirectory = true;
	private String delimiter;

	private ArrayList<String> classpathEntries = new ArrayList<String>();

	@Override
	public void configure(YarnEnvironmentBuilder builder) throws Exception {
		builder.addClasspathEntries(classpathEntries);
		builder.setUseDefaultYarnClasspath(useDefaultYarnClasspath);
		builder.setDefaultYarnAppClasspath(defaultYarnAppClasspath);
		builder.setIncludeBaseDirectory(includeBaseDirectory);
		if (StringUtils.hasText(delimiter)) {
			builder.setDelimiter(delimiter);
		}
	}

	@Override
	public EnvironmentClasspathConfigurer entry(String entry) {
		if (StringUtils.hasText(entry)) {
			classpathEntries.add(entry);
		}
		return this;
	}

	@Override
	public EnvironmentClasspathConfigurer entries(String... entries) {
		if (entries != null) {
			for (String entry : entries) {
				if (StringUtils.hasText(entry)) {
					classpathEntries.add(entry);
				}
			}
		}
		return this;
	}

	@Override
	public EnvironmentClasspathConfigurer entries(List<String> entries) {
		return entries(StringUtils.toStringArray(entries));
	}

	@Override
	public EnvironmentClasspathConfigurer useDefaultYarnClasspath(boolean defaultClasspath) {
		this.useDefaultYarnClasspath = defaultClasspath;
		return this;
	}

	@Override
	public EnvironmentClasspathConfigurer defaultYarnAppClasspath(String defaultClasspath) {
		this.defaultYarnAppClasspath = defaultClasspath;
		return this;
	}

	@Override
	public EnvironmentClasspathConfigurer defaultYarnAppClasspath(String... defaultClasspath) {
		this.defaultYarnAppClasspath = StringUtils.arrayToCommaDelimitedString(defaultClasspath);
		return this;
	}

	@Override
	public EnvironmentClasspathConfigurer includeBaseDirectory(boolean includeBaseDirectory) {
		this.includeBaseDirectory = includeBaseDirectory;
		return this;
	}

	@Override
	public EnvironmentClasspathConfigurer delimiter(String delimiter) {
		this.delimiter = delimiter;
		return this;
	}

}
