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
import org.apache.hadoop.security.SecurityUtil;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;

/**
 * FactoryBean for creating {@link Configuration} instances.
 *
 * @author Costin Leau
 * @author Janne Valkealahti
 *
 */
public class ConfigurationFactoryBean implements BeanClassLoaderAware, InitializingBean, FactoryBean<Configuration> {

	private static final Log log = LogFactory.getLog(ConfigurationFactoryBean.class);

	public static final String USERKEYTAB = "spring.hadoop.userKeytab";

	public static final String USERPRINCIPAL = "spring.hadoop.userPrincipal";

	private Configuration internalConfig;
	private Configuration configuration;
	private boolean loadDefaults = true;
	private Set<Resource> resources;
	private Properties properties;

	private ClassLoader beanClassLoader = getClass().getClassLoader();
	private boolean initialize = true;
	private boolean registerJvmUrl = false;

	private String fsUri;
	private String rmUri;
	private String jhUri;

	private String userKeytab;
	private String userPrincipal;
	private String namenodePrincipal;
	private String rmManagerPrincipal;
	private String securityMethod;

	@Override
	public void afterPropertiesSet() throws Exception {
		internalConfig = createConfiguration(configuration);

		internalConfig.setClassLoader(beanClassLoader);
		if (resources != null) {
			for (Resource resource : resources) {
				internalConfig.addResource(resource.getURL());
			}
		}

		ConfigurationUtils.addProperties(internalConfig, properties);

		// for below property values we can't use constants
		// from hadoop packages because we need to able to
		// compile for different versions.
		// set hdfs / fs URI last to override all other properties
		if (StringUtils.hasText(fsUri)) {
			internalConfig.set("fs.defaultFS", fsUri.trim());
		}

		if (StringUtils.hasText(rmUri)) {
			internalConfig.set("yarn.resourcemanager.address", rmUri.trim());
		}

		if (StringUtils.hasText(jhUri)) {
			internalConfig.set("mapreduce.jobhistory.address", jhUri.trim());
		}

		if (StringUtils.hasText(userKeytab)) {
			internalConfig.set(USERKEYTAB, userKeytab.trim());
		}

		if (StringUtils.hasText(userPrincipal)) {
			internalConfig.set(USERPRINCIPAL, userPrincipal.trim());
		}

		if (StringUtils.hasText(securityMethod)) {
			internalConfig.setBoolean("hadoop.security.authorization", true);
			internalConfig.set("hadoop.security.authentication", securityMethod);
			internalConfig.set("dfs.namenode.kerberos.principal", namenodePrincipal);
			internalConfig.set("yarn.resourcemanager.principal", rmManagerPrincipal);
			UserGroupInformation.setConfiguration(internalConfig);
			if (StringUtils.hasText(userKeytab) && StringUtils.hasText(userPrincipal)) {
				try {
					SecurityUtil.login(internalConfig, USERKEYTAB, USERPRINCIPAL);
				} catch (Exception e) {
					log.warn("Cannot login using keytab " + userKeytab + " and principal " + userPrincipal, e);
				}
			}
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
	 * Sets the used security method.
	 *
	 * @param securityMethod the security method
	 */
	public void setSecurityMethod(String securityMethod) {
		this.securityMethod = securityMethod;
	}

	@Override
	public Configuration getObject() {
		return internalConfig;
	}

	@Override
	public Class<?> getObjectType() {
		return (internalConfig != null ? internalConfig.getClass() : Configuration.class);
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	/**
	 * Sets a path to a keytab file.
	 *
	 * @param userKeytab the path to a keytab file
	 */
	public void setUserKeytab(String userKeytab) {
		this.userKeytab = userKeytab;
	}

	/**
	 * Sets the the user login principal.
	 *
	 * @param userPrincipal the user login principal
	 */
	public void setUserPrincipal(String userPrincipal) {
		this.userPrincipal = userPrincipal;
	}

	/**
	 * Sets the used namenode principal.
	 *
	 * @param namenodePrincipal the namenode principal.
	 */
	public void setNamenodePrincipal(String namenodePrincipal) {
		this.namenodePrincipal = namenodePrincipal;
	}

	/**
	 * Sets the used resource manager principal.
	 *
	 * @param rmManagerPrincipal the resource manager principal
	 */
	public void setRmManagerPrincipal(String rmManagerPrincipal) {
		this.rmManagerPrincipal = rmManagerPrincipal;
	}

	/**
	 * Creates a configuration instance potentially using the existing one (passed as an argument - which can be null).
	 *
	 * @param existing existing configuration
	 * @return configuration instance
	 */
	protected Configuration createConfiguration(Configuration existing) {
		return (existing != null ? new Configuration(existing) : new Configuration(loadDefaults));
	}


	protected void postProcessConfiguration(Configuration configuration) {
		// no-op
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

	/**
	 * Sets the File System ('fs.defaultFS') URI
	 *
	 * @param fsUri the default file system uri
	 */
	public void setFileSystemUri(String fsUri) {
		this.fsUri = fsUri;
	}

	/**
	 * Sets the Yarn resource manager ('yarn.resourcemanager.address') URI.
	 *
	 * @param rmUri the resource manager uri
	 */
	public void setRmManagerUri(String rmUri) {
		this.rmUri = rmUri;
	}

	/**
	 * Sets the Job Tracker ('mapreduce.jobhistory.address') URI.
	 *
	 * @param jhUri the job tracker uri
	 */
	public void setJobHistoryUri(String jhUri) {
		this.jhUri = jhUri;
	}

}