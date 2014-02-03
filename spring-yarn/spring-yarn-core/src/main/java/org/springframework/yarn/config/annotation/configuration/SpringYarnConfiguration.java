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
package org.springframework.yarn.config.annotation.configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.hadoop.config.common.annotation.AbstractAnnotationConfiguration;
import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.config.annotation.EnableYarn;
import org.springframework.yarn.config.annotation.SpringYarnConfigs;
import org.springframework.yarn.config.annotation.SpringYarnConfigurer;
import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.builders.SpringYarnConfigBuilder;
import org.springframework.yarn.event.DefaultYarnEventPublisher;
import org.springframework.yarn.event.YarnEventPublisher;
import org.springframework.yarn.fs.ResourceLocalizer;
import org.springframework.yarn.support.YarnUtils;

/**
 * Uses a {@link SpringYarnConfigBuilder} to create {@link SpringYarnConfigs}
 * holding all relevant configurations for Spring Yarn. It then exports the
 * necessary beans. Customisations can be made to {@link SpringYarnConfigBuilder} by
 * extending {@link SpringYarnConfigurerAdapter} and exposing it as a
 * {@link Configuration} or implementing {@link SpringYarnConfigurer} and
 * exposing it as a {@link Configuration}. This configuration is imported when
 * using {@link EnableYarn}.
 *
 * @author Janne Valkealahti
 * @see EnableYarn
 * @see SpringYarnConfigBuilder
 *
 */
@Configuration
public class SpringYarnConfiguration extends AbstractAnnotationConfiguration<SpringYarnConfigBuilder, SpringYarnConfigs> {

	private final static Log log = LogFactory.getLog(SpringYarnConfiguration.class);

	protected SpringYarnConfigBuilder builder = new SpringYarnConfigBuilder();

	@Autowired
	private ApplicationContext applicationContext;

	@Bean(name=YarnSystemConstants.DEFAULT_ID_EVENT_PUBLISHER)
	public YarnEventPublisher yarnEventPublisher() {
		return new DefaultYarnEventPublisher();
	}

	@Bean(name={YarnSystemConstants.DEFAULT_ID_CONFIGURATION, "hadoopConfiguration"})
	public YarnConfiguration yarnConfiguration() throws Exception {
		// TODO: make sure registering both names is ok
		maySetConfigurationFromContext();
		log.info("Building configuration for bean 'yarnConfiguration'");
		return (YarnConfiguration) builder.getOrBuild().getConfiguration();
	}

	/**
	 * Sets a configuration from an {@code ApplicationContext} if one and
	 * only one configuration of type {@code org.apache.hadoop.conf.Configuration}
	 * or {@code YarnConfiguration} is found. This is an indication that someone
	 * injected hadoop configuration before we get to this point in spring javaconfig.
	 */
	public void maySetConfigurationFromContext() {
		Map<String, org.apache.hadoop.conf.Configuration> vanillaConfigBeans =
				applicationContext.getBeansOfType(org.apache.hadoop.conf.Configuration.class);
		Map<String, YarnConfiguration> yarnConfigBeans =
				applicationContext.getBeansOfType(YarnConfiguration.class);

		if (log.isDebugEnabled()) {
			log.debug("vanillaConfigBeans: " + vanillaConfigBeans);
			for (org.apache.hadoop.conf.Configuration conf : vanillaConfigBeans.values()) {
				log.debug(YarnUtils.toString(conf));
			}
			log.debug("yarnConfigBeans: " + yarnConfigBeans);
			for (org.apache.hadoop.conf.Configuration conf : yarnConfigBeans.values()) {
				log.debug(YarnUtils.toString(conf));
			}
		}

		Map<String, org.apache.hadoop.conf.Configuration> uniqueBeans = new HashMap<String, org.apache.hadoop.conf.Configuration>();
		uniqueBeans.putAll(vanillaConfigBeans);
		uniqueBeans.putAll(yarnConfigBeans);

		if (uniqueBeans.size() == 1) {
			log.info("About to set SpringYarnConfigBuilder hadoop configuration");
			builder.setYarnConfiguration(uniqueBeans.entrySet().iterator().next().getValue());
		} else {
			log.info("We couldn't figure out if we could use existing configuration");
		}
	}

	@Bean(name=YarnSystemConstants.DEFAULT_ID_LOCAL_RESOURCES)
	@DependsOn(YarnSystemConstants.DEFAULT_ID_CONFIGURATION)
	public ResourceLocalizer yarnLocalresources() throws Exception {
		return builder.getOrBuild().getLocalizer();
	}

	@Bean(name=YarnSystemConstants.DEFAULT_ID_ENVIRONMENT)
	@DependsOn(YarnSystemConstants.DEFAULT_ID_CONFIGURATION)
	public Map<String, String> yarnEnvironment() throws Exception {
		return builder.getOrBuild().getEnvironment();
	}

	@Override
	protected void onConfigurers(List<AnnotationConfigurer<SpringYarnConfigs, SpringYarnConfigBuilder>> configurers) throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("onConfigurers: " + configurers);
		}
		for (AnnotationConfigurer<SpringYarnConfigs, SpringYarnConfigBuilder> configurer : configurers) {
			builder.apply(configurer);
		}
	}

	@Override
	public void setImportMetadata(AnnotationMetadata importMetadata) {
		super.setImportMetadata(importMetadata);
		Map<String, Object> enableYarnMap =
				importMetadata.getAnnotationAttributes(EnableYarn.class.getName());
		AnnotationAttributes enableYarnAttrs = AnnotationAttributes.fromMap(enableYarnMap);
		log.info("Enabling " + enableYarnAttrs.getEnum("enable") + " for Yarn");
	}

}
