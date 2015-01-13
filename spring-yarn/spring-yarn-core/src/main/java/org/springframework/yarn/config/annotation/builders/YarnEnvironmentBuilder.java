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
package org.springframework.yarn.config.annotation.builders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.hadoop.config.common.annotation.AbstractConfiguredAnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.ObjectPostProcessor;
import org.springframework.data.hadoop.config.common.annotation.configurers.DefaultPropertiesConfigurer;
import org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer;
import org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurerAware;
import org.springframework.util.StringUtils;
import org.springframework.yarn.config.annotation.configurers.DefaultEnvironmentClasspathConfigurer;
import org.springframework.yarn.config.annotation.configurers.EnvironmentClasspathConfigurer;
import org.springframework.yarn.configuration.EnvironmentFactoryBean;

/**
 * {@link AnnotationBuilder} for Yarn environment.
 *
 * @author Janne Valkealahti
 *
 */
public final class YarnEnvironmentBuilder
		extends AbstractConfiguredAnnotationBuilder<Map<String, Map<String, String>>, YarnEnvironmentConfigurer, YarnEnvironmentBuilder>
		implements PropertiesConfigurerAware, YarnEnvironmentConfigurer {

	private Configuration configuration;
	private final HashMap<String, DataHolder> datas = new HashMap<String, YarnEnvironmentBuilder.DataHolder>();

	/**
	 * Instantiates a new yarn environment builder.
	 */
	public YarnEnvironmentBuilder() {
		super(ObjectPostProcessor.QUIESCENT_POSTPROCESSOR, true);
	}

	@Override
	protected Map<String, Map<String, String>> performBuild() throws Exception {
		Map<String, Map<String, String>> envs = new HashMap<String, Map<String,String>>();
		for (Entry<String, DataHolder> entry : datas.entrySet()) {
			String id = entry.getKey();
			EnvironmentFactoryBean fb = new EnvironmentFactoryBean();
			fb.setConfiguration(configuration);
			fb.setProperties(getDataHolder(id).properties);
			fb.setClasspath(StringUtils.collectionToDelimitedString(getDataHolder(id).classpathEntries, getDataHolder(id).delimiter));
			fb.setDelimiter(getDataHolder(id).delimiter);
			fb.setDefaultYarnAppClasspath(getDataHolder(id).defaultYarnAppClasspath);
			fb.setDefaultMapreduceAppClasspath(getDataHolder(id).defaultMapreduceAppClasspath);
			fb.setUseDefaultYarnClasspath(getDataHolder(id).useDefaultYarnClasspath);
			fb.setUseDefaultMapreduceClasspath(getDataHolder(id).useDefaultMapreduceClasspath);
			fb.setIncludeLocalSystemEnv(getDataHolder(id).includeLocalSystemEnv);
			fb.setIncludeBaseDirectory(getDataHolder(id).includeBaseDirectory);
			fb.afterPropertiesSet();
			envs.put(id, fb.getObject());
		}
		return envs;
	}

	@Override
	public void configureProperties(Properties properties) {
		// TODO: missing id
		getDataHolder(null).properties.putAll(properties);
	}

	@Override
	public EnvironmentClasspathConfigurer withClasspath() throws Exception {
		return apply(new DefaultEnvironmentClasspathConfigurer());
	}

	@Override
	public EnvironmentClasspathConfigurer withClasspath(String id) throws Exception {
		return apply(new DefaultEnvironmentClasspathConfigurer(id));
	}

	@Override
	public YarnEnvironmentConfigurer entry(String key, String value) {
		return entry(null, key, value);
	}

	@Override
	public YarnEnvironmentConfigurer entry(String id, String key, String value) {
		getDataHolder(id).properties.put(key, value);
		return this;
	}

	@Override
	public YarnEnvironmentConfigurer propertiesLocation(String... locations) throws IOException {
		return propertiesLocationId(null, locations);
	}

	@Override
	public YarnEnvironmentConfigurer propertiesLocationId(String id, String[] locations) throws IOException {
		for (String location : locations) {
			PropertiesFactoryBean fb = new PropertiesFactoryBean();
			fb.setLocation(new ClassPathResource(location));
			fb.afterPropertiesSet();
			getDataHolder(id).properties.putAll(fb.getObject());
		}
		return this;
	}

	@Override
	public YarnEnvironmentConfigurer includeLocalSystemEnv(boolean includeLocalSystemEnv) {
		return includeLocalSystemEnv(null, includeLocalSystemEnv);
	}

	@Override
	public YarnEnvironmentConfigurer includeLocalSystemEnv(String id, boolean includeLocalSystemEnv) {
		getDataHolder(id).includeLocalSystemEnv = includeLocalSystemEnv;
		return this;
	}

	@Override
	public PropertiesConfigurer<YarnEnvironmentConfigurer> withProperties() throws Exception {
		return apply(new DefaultPropertiesConfigurer<Map<String, Map<String, String>>, YarnEnvironmentConfigurer, YarnEnvironmentBuilder>());
	}

	@Override
	public PropertiesConfigurer<YarnEnvironmentConfigurer> withProperties(String id) throws Exception {
		return apply(new DefaultPropertiesConfigurer<Map<String, Map<String, String>>, YarnEnvironmentConfigurer, YarnEnvironmentBuilder>());
	}

	/**
	 * Adds the classpath entries.
	 *
	 * @param id the id
	 * @param classpathEntries the classpath entries
	 */
	public void addClasspathEntries(String id, ArrayList<String> classpathEntries) {
		getDataHolder(id).classpathEntries.addAll(classpathEntries);
	}

	/**
	 * Sets the yarn configuration.
	 *
	 * @param configuration the yarn configuration
	 */
	public void configuration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the default classpath.
	 *
	 * @param id the id
	 * @param useDefaultClasspath the new default classpath
	 */
	public void setUseDefaultYarnClasspath(String id, boolean useDefaultClasspath) {
		getDataHolder(id).useDefaultYarnClasspath = useDefaultClasspath;
	}

	/**
	 * Sets the default classpath.
	 *
	 * @param id the id
	 * @param useDefaultClasspath the new default classpath
	 */
	public void setUseDefaultMapreduceClasspath(String id, boolean useDefaultClasspath) {
		getDataHolder(id).useDefaultMapreduceClasspath = useDefaultClasspath;
	}

	/**
	 * Sets the include base directory.
	 *
	 * @param id the id
	 * @param includeBaseDirectory the new include base directory
	 */
	public void setIncludeBaseDirectory(String id, boolean includeBaseDirectory) {
		getDataHolder(id).includeBaseDirectory = includeBaseDirectory;
	}

	/**
	 * Sets the delimiter.
	 *
	 * @param id the id
	 * @param delimiter the new delimiter
	 */
	public void setDelimiter(String id, String delimiter) {
		getDataHolder(id).delimiter = delimiter;
	}

	/**
	 * Sets the default yarn app classpath.
	 *
	 * @param id the id
	 * @param defaultYarnAppClasspath the new default yarn app classpath
	 */
	public void setDefaultYarnAppClasspath(String id, String defaultYarnAppClasspath) {
		getDataHolder(id).defaultYarnAppClasspath = defaultYarnAppClasspath;
	}

	/**
	 * Sets the default mr app classpath.
	 *
	 * @param id the id
	 * @param defaultMapreduceAppClasspath the new default mr app classpath
	 */
	public void setDefaultMapreduceAppClasspath(String id, String defaultMapreduceAppClasspath) {
		getDataHolder(id).defaultMapreduceAppClasspath = defaultMapreduceAppClasspath;
	}

	private synchronized DataHolder getDataHolder(String id) {
		DataHolder holder = datas.get(id);
		if (holder == null) {
			holder = new DataHolder();
			datas.put(id, holder);
		}
		return holder;
	}

	private static class DataHolder {
		private boolean useDefaultYarnClasspath = true;
		private boolean useDefaultMapreduceClasspath = true;
		private boolean includeBaseDirectory = true;
		private boolean includeLocalSystemEnv = false;
		private String defaultYarnAppClasspath;
		private String defaultMapreduceAppClasspath;
		private String delimiter = ":";
		private Properties properties = new Properties();
		private ArrayList<String> classpathEntries = new ArrayList<String>();
	}

}
