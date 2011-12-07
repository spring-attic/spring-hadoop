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

import java.net.URL;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
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

	private static final Log log = LogFactory.getLog(ConfigurationFactoryBean.class);

	private Configuration config;
	private Configuration configuration;
	private boolean loadDefaults = true;
	private Set<Resource> resources;
	private Map<String, Object> properties;

	private ClassLoader beanClassLoader = getClass().getClassLoader();
	private boolean initialize = true;
	private boolean registerJvmUrl = false;

	public void afterPropertiesSet() throws Exception {
		config = createConfiguration(configuration);

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

		postProcessConfiguration(config);

		if (registerJvmUrl) {
			try {
				URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(getObject()));
				log.info("Registered HDFS URL stream handler");
			} catch (Error err) {
				log.warn("Cannot register Hadoop URL stream handler - one is already registered");
			}
		}
	}

	/**
	 * Creates a configuration instance potentially using the existing one (passed as an argument - which can be null). 
	 * 
	 * @param existing
	 * @return configuration instance
	 */
	protected Configuration createConfiguration(Configuration existing) {
		return (existing != null ? new Configuration(existing) : new Configuration(loadDefaults));
	}


	protected void postProcessConfiguration(Configuration configuration) {
		// no-op
	}


	public Configuration getObject() {
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

	public void setRegisterUrlHandler(boolean register) {
		this.registerJvmUrl = register;
	}
}