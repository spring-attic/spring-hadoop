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
package org.springframework.yarn.configuration;

import java.net.URL;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;

/**
 * FactoryBean for creating {@link Configuration} instances.
 *
 * @author Costin Leau
 * @author Janne Valkealahti
 */
public class ConfigurationFactoryBean implements BeanClassLoaderAware, InitializingBean, FactoryBean<YarnConfiguration> {

	private static final Log log = LogFactory.getLog(ConfigurationFactoryBean.class);

	private YarnConfiguration internalConfig;
	private YarnConfiguration configuration;
	private Set<Resource> resources;
	private Properties properties;

	private ClassLoader beanClassLoader = getClass().getClassLoader();
	private boolean initialize = true;
	private boolean registerJvmUrl = false;

	private String fsUri;
	private String rmAddress;

	public void afterPropertiesSet() throws Exception {
		internalConfig = createConfiguration(configuration);

		internalConfig.setClassLoader(beanClassLoader);
		if (resources != null) {
			for (Resource resource : resources) {
				internalConfig.addResource(resource.getURL());
			}
		}

		ConfigurationUtils.addProperties(internalConfig, properties);

		// set hdfs / fs URI last to override all other properties
		if (StringUtils.hasText(fsUri)) {
			internalConfig.set("fs.default.name", fsUri.trim());
		}

		if (StringUtils.hasText(rmAddress)) {
			internalConfig.set("yarn.resourcemanager.address", rmAddress.trim());
		}

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
	protected YarnConfiguration createConfiguration(Configuration existing) {
		return (existing != null ? new YarnConfiguration(existing) : new YarnConfiguration());
	}


	protected void postProcessConfiguration(Configuration configuration) {
		// no-op
	}


	public YarnConfiguration getObject() {
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
	public void setConfiguration(YarnConfiguration configuration) {
		this.configuration = configuration;
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

	/**
	 * Sets the File System ('fs.default.name') URI.
	 *
	 * @param fsUri
	 */
	public void setFileSystemUri(String fsUri) {
		this.fsUri = fsUri;
	}

	/**
	 * Sets the Job Tracker ('mapred.jobtracker') URI.
	 *
	 * @param rmAddress
	 */
	public void setResourceManagerAddress(String rmAddress) {
		this.rmAddress = rmAddress;
	}
}
