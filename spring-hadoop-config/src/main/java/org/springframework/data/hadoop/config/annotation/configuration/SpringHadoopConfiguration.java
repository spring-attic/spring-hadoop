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
package org.springframework.data.hadoop.config.annotation.configuration;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.HadoopSystemConstants;
import org.springframework.data.hadoop.config.annotation.EnableHadoop;
import org.springframework.data.hadoop.config.annotation.SpringHadoopConfigs;
import org.springframework.data.hadoop.config.annotation.SpringHadoopConfigurer;
import org.springframework.data.hadoop.config.annotation.SpringHadoopConfigurerAdapter;
import org.springframework.data.hadoop.config.annotation.builders.SpringHadoopConfigBuilder;
import org.springframework.data.hadoop.config.common.annotation.AbstractAnnotationConfiguration;
import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer;

/**
 * Uses a {@link SpringHadoopConfigBuilder} to create {@link SpringHadoopConfigs}
 * holding all relevant configurations for Spring Hadoop. It then exports the
 * necessary beans. Customisations can be made to {@link SpringHadoopConfigBuilder} by
 * extending {@link SpringHadoopConfigurerAdapter} and exposing it as a
 * {@link Configuration} or implementing {@link SpringHadoopConfigurer} and
 * exposing it as a {@link Configuration}. This configuration is imported when
 * using {@link EnableHadoop}.
 *
 * @author Janne Valkealahti
 * @see EnableHadoop
 * @see SpringHadoopConfigBuilder
 *
 */
@Configuration
public class SpringHadoopConfiguration extends AbstractAnnotationConfiguration<SpringHadoopConfigBuilder, SpringHadoopConfigs> {

	private static final Log log = LogFactory.getLog(SpringHadoopConfiguration.class);

	protected final SpringHadoopConfigBuilder builder = new SpringHadoopConfigBuilder();

	@Override
	protected void onConfigurers(List<AnnotationConfigurer<SpringHadoopConfigs, SpringHadoopConfigBuilder>> configurers)
			throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("onConfigurers: " + configurers);
		}
		for (AnnotationConfigurer<SpringHadoopConfigs, SpringHadoopConfigBuilder> configurer : configurers) {
			builder.apply(configurer);
		}
	}

	@Bean(name=HadoopSystemConstants.DEFAULT_ID_CONFIGURATION)
	public org.apache.hadoop.conf.Configuration configuration() throws Exception {
		log.info("Building configuration for bean 'hadoopConfiguration'");
		return builder.getOrBuild().getConfiguration();
	}

}
