/*
 * Copyright 2014-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.boot;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.boot.properties.SpringHadoopProperties;
import org.springframework.data.hadoop.config.annotation.EnableHadoop;
import org.springframework.data.hadoop.config.annotation.SpringHadoopConfigurerAdapter;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigConfigurer;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Spring Hadoop.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@ConditionalOnClass(EnableHadoop.class)
@ConditionalOnMissingBean(org.apache.hadoop.conf.Configuration.class)
public class HadoopAutoConfiguration {

	@Configuration
	@EnableConfigurationProperties({ SpringHadoopProperties.class })
	@EnableHadoop
	public static class SpringHadoopConfig extends SpringHadoopConfigurerAdapter {

		@Autowired
		private SpringHadoopProperties shp;

		@Override
		public void configure(HadoopConfigConfigurer config) throws Exception {
			config
				.fileSystemUri(shp.getFsUri())
				.resourceManagerAddress(shp.getResourceManagerAddress())
				.jobHistoryAddress(shp.getJobHistoryAddress())
				.withProperties()
					.properties(shp.getConfig())
					.and()
				.withResources()
					.resources(shp.getResources())
					.and()
				.withSecurity()
					.namenodePrincipal(shp.getSecurity() != null ? shp.getSecurity().getNamenodePrincipal() : null)
					.rmManagerPrincipal(shp.getSecurity() != null ? shp.getSecurity().getRmManagerPrincipal() : null)
					.authMethod(shp.getSecurity() != null ? shp.getSecurity().getAuthMethod() : null)
					.userPrincipal(shp.getSecurity() != null ? shp.getSecurity().getUserPrincipal() : null)
					.userKeytab(shp.getSecurity() != null ? shp.getSecurity().getUserKeytab() : null);
		}

	}

}
