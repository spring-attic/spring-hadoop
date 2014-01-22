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
package org.springframework.yarn.config.annotation.builders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.hadoop.config.common.annotation.AbstractConfiguredAnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigure;
import org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigureAware;
import org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer;
import org.springframework.util.StringUtils;
import org.springframework.yarn.config.annotation.configurers.EnvironmentClasspathConfigure;
import org.springframework.yarn.config.annotation.configurers.EnvironmentClasspathConfigurer;
import org.springframework.yarn.configuration.EnvironmentFactoryBean;

/**
 * {@link AnnotationBuilder} for Yarn environment.
 *
 * @author Janne Valkealahti
 *
 */
public final class YarnEnvironmentBuilder
		extends AbstractConfiguredAnnotationBuilder<Map<String, String>, YarnEnvironmentConfigure, YarnEnvironmentBuilder>
		implements PropertiesConfigureAware, YarnEnvironmentConfigure {

	private boolean defaultClasspath = true;
	private boolean includeBaseDirectory = true;
	private boolean includeSystemEnv = true;
	private String delimiter = ":";
	private Properties properties = new Properties();
	private ArrayList<String> classpathEntries = new ArrayList<String>();

	/**
	 * Instantiates a new yarn environment builder.
	 */
	public YarnEnvironmentBuilder() {}

	@Override
	protected Map<String, String> performBuild() throws Exception {
		EnvironmentFactoryBean fb = new EnvironmentFactoryBean();
		fb.setProperties(properties);
		fb.setClasspath(StringUtils.collectionToDelimitedString(classpathEntries, delimiter));
		fb.setDelimiter(delimiter);
		fb.setDefaultYarnAppClasspath(defaultClasspath);
		fb.setIncludeSystemEnv(includeSystemEnv);
		fb.setIncludeBaseDirectory(includeBaseDirectory);
		fb.afterPropertiesSet();
		return fb.getObject();
	}

	@Override
	public void configureProperties(Properties properties) {
		this.properties.putAll(properties);
	}

	@Override
	public EnvironmentClasspathConfigure withClasspath() throws Exception {
		return apply(new EnvironmentClasspathConfigurer());
	}

	@Override
	public YarnEnvironmentConfigure entry(String key, String value) {
		properties.put(key, value);
		return this;
	}

	@Override
	public YarnEnvironmentConfigure propertiesLocation(String... locations) throws IOException {
		for (String location : locations) {
			PropertiesFactoryBean fb = new PropertiesFactoryBean();
			fb.setLocation(new ClassPathResource(location));
			fb.afterPropertiesSet();
			properties.putAll(fb.getObject());
		}
		return this;
	}

	@Override
	public YarnEnvironmentConfigure includeSystemEnv(boolean includeSystemEnv) {
		this.includeSystemEnv = includeSystemEnv;
		return this;
	}

	@Override
	public PropertiesConfigure<YarnEnvironmentConfigure> withProperties() throws Exception {
		return apply(new PropertiesConfigurer<Map<String, String>, YarnEnvironmentConfigure, YarnEnvironmentBuilder>());
	}

	/**
	 * Adds the classpath entries.
	 *
	 * @param classpathEntries the classpath entries
	 */
	public void addClasspathEntries(ArrayList<String> classpathEntries) {
		this.classpathEntries.addAll(classpathEntries);
	}

	/**
	 * Sets the default classpath.
	 *
	 * @param defaultClasspath the new default classpath
	 */
	public void setDefaultClasspath(boolean defaultClasspath) {
		this.defaultClasspath = defaultClasspath;
	}

	/**
	 * Sets the include base directory.
	 *
	 * @param includeBaseDirectory the new include base directory
	 */
	public void setIncludeBaseDirectory(boolean includeBaseDirectory) {
		this.includeBaseDirectory = includeBaseDirectory;
	}

	/**
	 * Sets the delimiter.
	 *
	 * @param delimiter the new delimiter
	 */
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

}
