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
package org.springframework.yarn.configuration;

import java.net.URL;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.security.SecurityAuthMethod;
import org.springframework.util.StringUtils;

/**
 * FactoryBean for creating {@link YarnConfiguration} instances.
 *
 * @author Costin Leau
 * @author Janne Valkealahti
 *
 */
public class ConfigurationFactoryBean implements BeanClassLoaderAware, InitializingBean, FactoryBean<YarnConfiguration> {

	private static final Log log = LogFactory.getLog(ConfigurationFactoryBean.class);

	public static final String USERKEYTAB = "spring.hadoop.userKeytab";

	public static final String USERPRINCIPAL = "spring.hadoop.userPrincipal";

	/** Configuration built internally and the one returned from this factory */
	private YarnConfiguration internalConfig;

	/** Considered as parent configuration when initial internalConfig is created */
	private YarnConfiguration configuration;

	/** Resources added into config as supported by Hadoop Configuration class */
	private Set<Resource> resources;

	/** Properties added into configuration */
	private Properties properties;

	private ClassLoader beanClassLoader = getClass().getClassLoader();
	private boolean initialize = true;
	private boolean registerJvmUrl = false;

	private SecurityAuthMethod securityAuthMethod;
	private String userPrincipal;
	private String userKeytab;
	private String namenodePrincipal;
	private String rmManagerPrincipal;

	private String fsUri;
	private String rmAddress;
	private String schedulerAddress;

	@Override
	public YarnConfiguration getObject() {
		return internalConfig;
	}

	@Override
	public Class<YarnConfiguration> getObjectType() {
		return YarnConfiguration.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

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

		// set hdfs / fs URI last to override all other properties
		if (StringUtils.hasText(fsUri)) {
			log.info("Overwriting fsUri=[" + internalConfig.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY) + "] with fsUri=[" +fsUri.trim() + "]");
			internalConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsUri.trim());
		}

		if (StringUtils.hasText(rmAddress)) {
			log.info("Overwriting rmAddress=[" + internalConfig.get(YarnConfiguration.RM_ADDRESS) + "] with rmAddress=[" +rmAddress.trim() + "]");
			internalConfig.set(YarnConfiguration.RM_ADDRESS, rmAddress.trim());
		}

		if (StringUtils.hasText(schedulerAddress)) {
			internalConfig.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, schedulerAddress.trim());
		}

		if (StringUtils.hasText(userKeytab)) {
			internalConfig.set(USERKEYTAB, userKeytab.trim());
		}

		if (StringUtils.hasText(userPrincipal)) {
			internalConfig.set(USERPRINCIPAL, userPrincipal.trim());
		}

		if (initialize) {
			internalConfig.size();
		}

		if (securityAuthMethod != null) {
			log.info("Enabling security using kerberos");
			internalConfig.setBoolean("hadoop.security.authorization", true);
			internalConfig.set("hadoop.security.authentication", "kerberos");
			if (StringUtils.hasText(namenodePrincipal)) {
				log.info("Using dfs.namenode.kerberos.principal=" + namenodePrincipal);
				internalConfig.set("dfs.namenode.kerberos.principal", namenodePrincipal);
			}
			if (StringUtils.hasText(rmManagerPrincipal)) {
				log.info("Using yarn.resourcemanager.principal=" + rmManagerPrincipal);
				internalConfig.set("yarn.resourcemanager.principal", rmManagerPrincipal);
			}
		}

		boolean ugiCalled = false;
		if (registerJvmUrl) {
			try {
				// force UGI init to prevent infinite loop - see SHDP-92
				UserGroupInformation.setConfiguration(internalConfig);
				ugiCalled = true;
				URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(getObject()));
				log.info("Registered HDFS URL stream handler");
			} catch (Error err) {
				log.warn("Cannot register Hadoop URL stream handler - one is already registered");
			}
		}

		// don't call twice
		if (!ugiCalled) {
			UserGroupInformation.setConfiguration(internalConfig);
		}

		if (StringUtils.hasText(userKeytab) && StringUtils.hasText(userPrincipal)) {
			try {
				SecurityUtil.login(internalConfig, USERKEYTAB, USERPRINCIPAL);
				log.info("Login using keytab " + userKeytab + " and principal " + userPrincipal);
			} catch (Exception e) {
				log.warn("Cannot login using keytab " + userKeytab + " and principal " + userPrincipal, e);
			}
		}

		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
		log.info("Executing with tokens:");
		for (Token<?> token : credentials.getAllTokens()) {
			log.info(token);
		}

	}

	/**
	 * Sets the security auth method.
	 *
	 * @param securityAuthMethod the new security auth method
	 */
	public void setSecurityAuthMethod(SecurityAuthMethod securityAuthMethod) {
		this.securityAuthMethod = securityAuthMethod;
	}

	/**
	 * Sets the user principal.
	 *
	 * @param userPrincipal the new user principal
	 */
	public void setUserPrincipal(String userPrincipal) {
		this.userPrincipal = userPrincipal;
	}

	/**
	 * Sets the user keytab.
	 *
	 * @param userKeytab the new user keytab
	 */
	public void setUserKeytab(String userKeytab) {
		this.userKeytab = userKeytab;
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
	 * Sets the File System ('fs.defaultFS') URI.
	 *
	 * @param fsUri the file system uri
	 */
	public void setFsUri(String fsUri) {
		this.fsUri = fsUri;
	}

	/**
	 * Sets the Yarn ('yarn.resourcemanager.address') address.
	 *
	 * @param rmAddress the resource manager address
	 */
	public void setRmAddress(String rmAddress) {
		this.rmAddress = rmAddress;
	}

	/**
	 * Sets the Yarn ('yarn.resourcemanager.scheduler.address') address.
	 *
	 * @param schedulerAddress the resource manager scheduler address
	 */
	public void setSchedulerAddress(String schedulerAddress) {
		this.schedulerAddress = schedulerAddress;
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
	 * Creates a configuration instance potentially using the existing one (passed as an argument - which can be null).
	 *
	 * @param existing the configuration
	 * @return configuration new or wrapped configuration
	 */
	protected YarnConfiguration createConfiguration(Configuration existing) {
		return (existing != null ? new YarnConfiguration(existing) : new YarnConfiguration());
	}

}
