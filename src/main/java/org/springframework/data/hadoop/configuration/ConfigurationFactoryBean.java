/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.hadoop.configuration;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

/**
 * FactoryBean for creating {@link Configuration} instances.
 * 
 * @author Costin Leau
 */
public class ConfigurationFactoryBean implements BeanClassLoaderAware, InitializingBean, FactoryBean<Configuration> {

	private Configuration config;
	private Configuration configuration;
	private boolean loadDefaults = true;
	private Set<Resource> resources;
	private Map<String, Object> properties;

	private ClassLoader beanClassLoader;
	private boolean initialize = true;

	public void afterPropertiesSet() throws Exception {
		config = (configuration != null ? new Configuration(configuration) : new Configuration(loadDefaults));

		config.setClassLoader(beanClassLoader);
		if (resources != null) {
			for (Resource resource : resources) {
				config.addResource(resource.getInputStream());
			}
		}

		if (properties != null) {
			for (Map.Entry<String, Object> entry : properties.entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();

				// so much for method overloading
				if (value instanceof Float) {
					config.setFloat(key, (Float) value);
				}
				if (value instanceof Long) {
					config.setLong(key, (Long) value);
				}
				if (value instanceof Integer) {
					config.setInt(key, (Integer) value);
				}
				if (value instanceof Boolean) {
					config.setBoolean(key, (Boolean) value);
				}
				if (value instanceof String[]) {
					config.setStrings(key, (String[]) value);
				}

				// fall back to toString
				config.set(key, value.toString());
			}
		}

		if (initialize) {
			config.size();
		}
	}

	public Configuration getObject() throws Exception {
		return config;
	}

	public Class<?> getObjectType() {
		return (config != null ? config.getClass() : Configuration.class);
	}

	public boolean isSingleton() {
		return true;
	}

	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}


	/**
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * @param loadDefaults The loadDefaults to set.
	 */
	public void setLoadDefaults(boolean loadDefaults) {
		this.loadDefaults = loadDefaults;
	}

	/**
	 * @param resources The resources to set.
	 */
	public void setResources(Set<Resource> resources) {
		this.resources = resources;
	}

	/**
	 * @param properties The properties to set.
	 */
	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;
	}

	/**
	 * @param initialize The initialize to set.
	 */
	public void setInitialize(boolean initialize) {
		this.initialize = initialize;
	}
}