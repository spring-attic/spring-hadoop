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
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.security.UserGroupInformation;
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

	private Configuration internalConfig;
	private Configuration configuration;
	private boolean loadDefaults = true;
	private Set<Resource> resources;
	private Properties properties;

	private ClassLoader beanClassLoader = getClass().getClassLoader();
	private boolean initialize = true;
	private boolean registerJvmUrl = false;

	public void afterPropertiesSet() throws Exception {
		internalConfig = createConfiguration(configuration);

		internalConfig.setClassLoader(beanClassLoader);
		if (resources != null) {
			for (Resource resource : resources) {
				internalConfig.addResource(resource.getURL());
			}
		}

		ConfigurationUtils.addProperties(internalConfig, properties);

		if (initialize) {
			internalConfig.size();
		}

		postProcessConfiguration(internalConfig);

		if (registerJvmUrl) {
			try {
				// force UGI init to prevent infinite loop - see SHDP-92
				UserGroupInformation.setConfiguration(internalConfig);
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
		return internalConfig;
	}

	public Class<?> getObjectType() {
		return (internalConfig != null ? internalConfig.getClass() : Configuration.class);
	}

	public boolean isSingleton() {
		return true;
	}

	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}


	/**
	 * Sets the parent configuration.
	 * 
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Indicates whether to load the defaults (the default) or not for this configuration.
	 * 
	 * @param loadDefaults The loadDefaults to set.
	 */
	public void setLoadDefaults(boolean loadDefaults) {
		this.loadDefaults = loadDefaults;
	}

	/**
	 * Sets the configuration resources.
	 * 
	 * @param resources The resources to set.
	 */
	public void setResources(Set<Resource> resources) {
		this.resources = resources;
	}

	/**
	 * Sets the configuration properties.
	 * 
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	/**
	 * Indicates whether the configuration object should be initialized (true) or not.
	 * This option should normally be set to true (the default) as it causes the jars, streams and resources
	 * set to be loaded - postponing the initializing might cause these to become unreadable.
	 * 
	 * @param initialize whether to initialize or not.
	 */
	public void setInitialize(boolean initialize) {
		this.initialize = initialize;
	}

	/**
	 * Indicates whether the configuration should register an URL handler (for allowing urls
	 * to understand HDFS prefixes, such as hdfs) or not. As this operation impacts an entire VM
	 * and can be invoked at most once per JVM, by default it is false.
	 * 
	 * @param register whether to register an URL handler or not
	 */
	public void setRegisterUrlHandler(boolean register) {
		this.registerJvmUrl = register;
	}
}