/*
 * Copyright 2014-2015 the original author or authors.
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
package org.springframework.data.hadoop.config.annotation.builders;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.config.common.annotation.AbstractConfiguredAnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.ObjectPostProcessor;
import org.springframework.data.hadoop.config.common.annotation.configurers.DefaultPropertiesConfigurer;
import org.springframework.data.hadoop.config.common.annotation.configurers.DefaultResourceConfigurer;
import org.springframework.data.hadoop.config.common.annotation.configurers.DefaultSecurityConfigurer;
import org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurer;
import org.springframework.data.hadoop.config.common.annotation.configurers.PropertiesConfigurerAware;
import org.springframework.data.hadoop.config.common.annotation.configurers.ResourceConfigurer;
import org.springframework.data.hadoop.config.common.annotation.configurers.ResourceConfigurerAware;
import org.springframework.data.hadoop.config.common.annotation.configurers.SecurityConfigurer;
import org.springframework.data.hadoop.config.common.annotation.configurers.SecurityConfigurerAware;
import org.springframework.data.hadoop.configuration.ConfigurationFactoryBean;
import org.springframework.data.hadoop.security.HadoopSecurity;
import org.springframework.data.hadoop.security.SecurityAuthMethod;

/**
 * {@link AnnotationBuilder} for {@link Configuration}.
 *
 * @author Janne Valkealahti
 *
 */
public final class HadoopConfigBuilder extends
		AbstractConfiguredAnnotationBuilder<Configuration, HadoopConfigConfigurer, HadoopConfigBuilder> implements
		PropertiesConfigurerAware, ResourceConfigurerAware, SecurityConfigurerAware, HadoopConfigConfigurer {

	private final Set<Resource> resources = new HashSet<Resource>();
	private final Properties properties = new Properties();
	private HadoopSecurity hadoopSecurity;
	private String fileSystemUri;
	private String rmAddress;
	private String jobHistoryAddress;
	private boolean loadDefaults = true;

	/**
	 * Instantiates a new yarn config builder.
	 */
	public HadoopConfigBuilder() {}

	/**
	 * Instantiates a new hadoop config builder.
	 *
	 * @param objectPostProcessor the object post processor
	 */
	public HadoopConfigBuilder(ObjectPostProcessor<Object> objectPostProcessor) {
		super(objectPostProcessor);
	}

	@Override
	protected Configuration performBuild() throws Exception {
		ConfigurationFactoryBean fb = new ConfigurationFactoryBean();

		if (!loadDefaults) {
			fb.setConfiguration(new Configuration(false));
		}

		fb.setResources(resources);
		fb.setProperties(properties);
		fb.setFileSystemUri(fileSystemUri);
		fb.setRmManagerUri(rmAddress);
		fb.setJobHistoryUri(jobHistoryAddress);

		if (hadoopSecurity != null) {
			SecurityAuthMethod securityAuthMethod = hadoopSecurity.getSecurityAuthMethod();
			if (securityAuthMethod != null) {
				fb.setSecurityMethod(securityAuthMethod.toString().toLowerCase());
			}
			fb.setUserPrincipal(hadoopSecurity.getUserPrincipal());
			fb.setUserKeytab(hadoopSecurity.getUserKeytab());
			fb.setNamenodePrincipal(hadoopSecurity.getNamenodePrincipal());
			fb.setRmManagerPrincipal(hadoopSecurity.getRmManagerPrincipal());
		}

		fb.afterPropertiesSet();

		Configuration c = fb.getObject();
		c = postProcess(c);
		return c;
	}

	@Override
	public void configureProperties(Properties properties) {
		getProperties().putAll(properties);
	}

	@Override
	public void configureResources(Set<Resource> resources) {
		getResources().addAll(resources);
	}

	@Override
	public void configureSecurity(HadoopSecurity hadoopSecurity) {
		this.hadoopSecurity = hadoopSecurity;
	}

	@Override
	public ResourceConfigurer<HadoopConfigConfigurer> withResources() throws Exception {
		return apply(new DefaultResourceConfigurer<Configuration, HadoopConfigConfigurer, HadoopConfigBuilder>());
	}

	@Override
	public PropertiesConfigurer<HadoopConfigConfigurer> withProperties() throws Exception {
		return apply(new DefaultPropertiesConfigurer<Configuration, HadoopConfigConfigurer, HadoopConfigBuilder>());
	}

	@Override
	public SecurityConfigurer<HadoopConfigConfigurer> withSecurity() throws Exception {
		return apply(new DefaultSecurityConfigurer<Configuration, HadoopConfigConfigurer, HadoopConfigBuilder>());
	}

	@Override
	public HadoopConfigConfigurer fileSystemUri(String uri) {
		fileSystemUri = uri;
		return this;
	}

	@Override
	public HadoopConfigConfigurer resourceManagerAddress(String address) {
		rmAddress = address;
		return this;
	}

	@Override
	public HadoopConfigConfigurer jobHistoryAddress(String jobHistoryAddress) {
		this.jobHistoryAddress = jobHistoryAddress;
		return this;
	}

	@Override
	public HadoopConfigConfigurer loadDefaults(boolean loadDefaults) {
		this.loadDefaults = loadDefaults;
		return this;
	}

	/**
	 * Gets the {@link Properties}.
	 *
	 * @return the properties
	 */
	public Properties getProperties() {
		return properties;
	}

	/**
	 * Gets the {@link Resource}s.
	 *
	 * @return the resources
	 */
	public Set<Resource> getResources() {
		return resources;
	}

	/**
	 * Gets the {@link HadoopSecurity}.
	 *
	 * @return the security
	 */
	public HadoopSecurity getSecurity() {
		return hadoopSecurity;
	}

}
