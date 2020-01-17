/*
 * Copyright 2013-2016 the original author or authors.
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
package org.springframework.yarn.boot;

import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.util.net.DefaultHostInfoDiscovery;
import org.springframework.data.hadoop.util.net.HostInfoDiscovery;
import org.springframework.util.StringUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.boot.condition.ConditionalOnYarnContainer;
import org.springframework.yarn.boot.properties.SpringHadoopProperties;
import org.springframework.yarn.boot.properties.SpringYarnContainerProperties;
import org.springframework.yarn.boot.properties.SpringYarnEnvProperties;
import org.springframework.yarn.boot.properties.SpringYarnHostInfoDiscoveryProperties;
import org.springframework.yarn.boot.properties.SpringYarnProperties;
import org.springframework.yarn.boot.support.ContainerLauncherRunner;
import org.springframework.yarn.boot.support.ContainerRegistrar;
import org.springframework.yarn.config.annotation.EnableYarn;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.builders.YarnConfigConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnContainerConfigurer;
import org.springframework.yarn.container.YarnContainer;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Spring Hadoop Yarn container.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@ConditionalOnProperty(prefix = "spring.yarn.container", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnYarnContainer
@ConditionalOnClass(EnableYarn.class)
@ConditionalOnMissingBean(YarnContainer.class)
public class YarnContainerAutoConfiguration {

	@Configuration
	@ConditionalOnProperty(prefix = "spring.yarn.container.runner", name = "enabled", havingValue = "true", matchIfMissing = true)
	@EnableConfigurationProperties({ SpringYarnProperties.class, SpringYarnContainerProperties.class })
	public static class RunnerConfig {

		@Autowired
		private SpringYarnContainerProperties sycp;

		@Bean
		@ConditionalOnMissingBean(ContainerLauncherRunner.class)
		@ConditionalOnClass(YarnContainer.class)
		public ContainerLauncherRunner containerLauncherRunner() {
			ContainerLauncherRunner runner = new ContainerLauncherRunner();
			runner.setWaitLatch(sycp.isKeepContextAlive());
			return runner;
		}
	}

	@Configuration
	@ConditionalOnClass(JobLauncher.class)
	@ConditionalOnExpression("${spring.yarn.batch.enabled:false}")
	public static class RuntimeConfig {

		@Bean
		public String customContainerClass() {
			// class reference would fail if not in classpath
			return "org.springframework.yarn.batch.container.DefaultBatchYarnContainer";
		}

	}

	@Configuration
	@ConditionalOnExpression("${endpoints.shutdown.enabled:false}")
	@EnableConfigurationProperties({ SpringYarnEnvProperties.class, SpringYarnHostInfoDiscoveryProperties.class })
	public static class ContainerRegistrarConfig {

		@Autowired
		private SpringYarnEnvProperties syep;

		@Autowired
		private SpringYarnHostInfoDiscoveryProperties syhidp;

		@Bean
		@ConditionalOnMissingBean(HostInfoDiscovery.class)
		public HostInfoDiscovery hostInfoDiscovery() {
			DefaultHostInfoDiscovery discovery = new DefaultHostInfoDiscovery();
			if (StringUtils.hasText(syhidp.getMatchIpv4())) {
				discovery.setMatchIpv4(syhidp.getMatchIpv4());
			}
			if (StringUtils.hasText(syhidp.getMatchInterface())) {
				discovery.setMatchInterface(syhidp.getMatchInterface());
			}
			if (syhidp.getPreferInterface() != null) {
				discovery.setPreferInterface(syhidp.getPreferInterface());
			}
			discovery.setLoopback(syhidp.isLoopback());
			discovery.setPointToPoint(syhidp.isPointToPoint());
			return discovery;
		}

		@Bean
		public ContainerRegistrar containerRegistrar(HostInfoDiscovery hostInfoDiscovery) {
			// only enable if boot shutdown is functional
			return new ContainerRegistrar(syep.getTrackUrl(), syep.getContainerId(), hostInfoDiscovery);
		}

	}

	@Configuration
	@EnableConfigurationProperties({ SpringHadoopProperties.class, SpringYarnProperties.class,
			SpringYarnEnvProperties.class, SpringYarnContainerProperties.class })
	@EnableYarn(enable=Enable.CONTAINER)
	public static class SpringYarnConfig extends SpringYarnConfigurerAdapter {

		@Autowired
		private SpringHadoopProperties shp;

		@Autowired
		private SpringYarnContainerProperties sycp;

		@Autowired(required=false)
		@Qualifier(YarnSystemConstants.DEFAULT_ID_CONTAINER_CLASS)
		private Class<? extends YarnContainer> yarnContainerClass;

		@Autowired(required=false)
		@Qualifier(YarnSystemConstants.DEFAULT_ID_CONTAINER_REF)
		private Object yarnContainerRef;

		@Autowired(required=false)
		@Qualifier("customContainerClass")
		private String containerClass;

		@Override
		public void configure(YarnConfigConfigurer config) throws Exception {
			config
				.fileSystemUri(shp.getFsUri())
				.withProperties()
					.properties(shp.getConfig())
					.and()
				.withSecurity()
					.namenodePrincipal(shp.getSecurity() != null ? shp.getSecurity().getNamenodePrincipal() : null)
					.rmManagerPrincipal(shp.getSecurity() != null ? shp.getSecurity().getRmManagerPrincipal() : null)
					.authMethod(shp.getSecurity() != null ? shp.getSecurity().getAuthMethod() : null)
					.userPrincipal(shp.getSecurity() != null ? shp.getSecurity().getUserPrincipal() : null)
					.userKeytab(shp.getSecurity() != null ? shp.getSecurity().getUserKeytab() : null);
		}

		@Override
		public void configure(YarnContainerConfigurer container) throws Exception {
			if (StringUtils.hasText(sycp.getContainerClass())) {
				container
					.containerClass(sycp.getContainerClass());
			} else if (yarnContainerClass != null){
				container
					.containerClass(yarnContainerClass);
			} else if (yarnContainerRef != null) {
				if (yarnContainerRef instanceof YarnContainer) {
					container
						.containerRef((YarnContainer) yarnContainerRef);
				}
			} else if (StringUtils.hasText(containerClass)) {
				container.containerClass(containerClass);
			}
		}
	}

}
