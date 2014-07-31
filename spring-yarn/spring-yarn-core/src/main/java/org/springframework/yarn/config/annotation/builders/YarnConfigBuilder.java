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

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
import org.springframework.data.hadoop.security.HadoopSecurity;
import org.springframework.yarn.configuration.ConfigurationFactoryBean;

/**
 * {@link AnnotationBuilder} for {@link YarnConfiguration}.
 *
 * @author Janne Valkealahti
 *
 */
public final class YarnConfigBuilder
		extends AbstractConfiguredAnnotationBuilder<YarnConfiguration,YarnConfigConfigurer,YarnConfigBuilder>
		implements PropertiesConfigurerAware, ResourceConfigurerAware, SecurityConfigurerAware, YarnConfigConfigurer {

	private final Set<Resource> resources = new HashSet<Resource>();
	private final Properties properties = new Properties();
	private HadoopSecurity hadoopSecurity;
	private String fileSystemUri;
	private String rmAddress;
	private String schedulerAddress;
	private boolean loadDefaults = true;

	/**
	 * Instantiates a new yarn config builder.
	 */
	public YarnConfigBuilder() {}

	/**
	 * Instantiates a new yarn config builder.
	 *
	 * @param objectPostProcessor the object post processor
	 */
	public YarnConfigBuilder(ObjectPostProcessor<Object> objectPostProcessor) {
		super(objectPostProcessor);
	}

	@Override
	protected YarnConfiguration performBuild() throws Exception {
		ConfigurationFactoryBean fb = new ConfigurationFactoryBean();

		if (!loadDefaults) {
			fb.setConfiguration(new YarnConfiguration(new Configuration(false)));
		}

		fb.setResources(resources);
		fb.setProperties(properties);
		fb.setFsUri(fileSystemUri);
		fb.setRmAddress(rmAddress);
		fb.setSchedulerAddress(schedulerAddress);

		if (hadoopSecurity != null) {
			fb.setSecurityAuthMethod(hadoopSecurity.getSecurityAuthMethod());
			fb.setUserPrincipal(hadoopSecurity.getUserPrincipal());
			fb.setUserKeytab(hadoopSecurity.getUserKeytab());
			fb.setNamenodePrincipal(hadoopSecurity.getNamenodePrincipal());
			fb.setRmManagerPrincipal(hadoopSecurity.getRmManagerPrincipal());
		}

		fb.afterPropertiesSet();

		YarnConfiguration c = fb.getObject();
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
	public ResourceConfigurer<YarnConfigConfigurer> withResources() throws Exception {
		return apply(new DefaultResourceConfigurer<YarnConfiguration, YarnConfigConfigurer, YarnConfigBuilder>());
	}

	@Override
	public PropertiesConfigurer<YarnConfigConfigurer> withProperties() throws Exception {
		return apply(new DefaultPropertiesConfigurer<YarnConfiguration, YarnConfigConfigurer, YarnConfigBuilder>());
	}

	@Override
	public SecurityConfigurer<YarnConfigConfigurer> withSecurity() throws Exception {
		return apply(new DefaultSecurityConfigurer<YarnConfiguration, YarnConfigConfigurer, YarnConfigBuilder>());
	}

	@Override
	public YarnConfigConfigurer fileSystemUri(String uri) {
		fileSystemUri = uri;
		return this;
	}

	@Override
	public YarnConfigConfigurer resourceManagerAddress(String address) {
		rmAddress = address;
		return this;
	}

	@Override
	public YarnConfigConfigurer schedulerAddress(String address) {
		schedulerAddress = address;
		return this;
	}

	@Override
	public YarnConfigConfigurer loadDefaults(boolean loadDefaults) {
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
