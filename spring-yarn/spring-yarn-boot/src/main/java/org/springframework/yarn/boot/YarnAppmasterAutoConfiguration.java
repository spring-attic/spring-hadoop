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
package org.springframework.yarn.boot;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.boot.condition.ConditionalOnYarnAppmaster;
import org.springframework.yarn.boot.support.AppmasterLauncherRunner;
import org.springframework.yarn.boot.support.SpringYarnAppmasterProperties;
import org.springframework.yarn.boot.support.SpringYarnEnvProperties;
import org.springframework.yarn.boot.support.SpringYarnProperties;
import org.springframework.yarn.config.annotation.EnableYarn;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnConfigConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerConfigurer;
import org.springframework.yarn.launch.LaunchCommandsFactoryBean;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Spring Hadoop Yarn
 * {@link YarnAppmaster}.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@ConditionalOnYarnAppmaster
@ConditionalOnClass(EnableYarn.class)
@ConditionalOnMissingBean(YarnAppmaster.class)
public class YarnAppmasterAutoConfiguration {

	private final static Log log = LogFactory.getLog(YarnAppmasterAutoConfiguration.class);

	@Configuration
	@EnableConfigurationProperties({SpringYarnProperties.class, SpringYarnAppmasterProperties.class})
	public static class RunnerConfig {

		@Autowired
		private SpringYarnAppmasterProperties syap;

		@Bean
		@ConditionalOnMissingBean(AppmasterLauncherRunner.class)
		@ConditionalOnBean(YarnAppmaster.class)
		public AppmasterLauncherRunner appmasterLauncherRunner() {
			AppmasterLauncherRunner runner = new AppmasterLauncherRunner();
			runner.setWaitLatch(syap.isWaitLatch());
			runner.setContainerCount(syap.getContainerCount());
			return runner;
		}
	}

	@Configuration
	@ConditionalOnClass(JobLauncher.class)
	@ConditionalOnExpression("${spring.yarn.batch.job.enabled:false}")
	public static class RuntimeConfig {

		@Bean
		public String customAppmasterClass() {
			return "org.springframework.yarn.batch.am.BatchAppmaster";
		}
	}


	@Configuration
	@EnableConfigurationProperties({SpringYarnProperties.class, SpringYarnAppmasterProperties.class, SpringYarnEnvProperties.class})
	@EnableYarn(enable=Enable.APPMASTER)
	static class Config extends SpringYarnConfigurerAdapter {

		@Autowired
		private SpringYarnProperties syp;

		@Autowired
		private SpringYarnAppmasterProperties syap;

		@Autowired(required=false)
		@Qualifier("customAppmasterClass")
		private String appmasterClass;

		@Override
		public void configure(YarnConfigConfigurer config) throws Exception {
			log.info("Configuring fsUri=[" + syp.getFsUri() + "]");
			log.info("Configuring rmAddress=[" + syp.getRmAddress() + "]");
			config
				.fileSystemUri(syp.getFsUri())
				.resourceManagerAddress(syp.getRmAddress())
				.schedulerAddress(syap.getRmSchedulerAddress());
		}

		@Override
		public void configure(YarnResourceLocalizerConfigurer localizer) throws Exception {
			localizer
				.withHdfs()
					.hdfs(syp.getApplicationDir() + "application.properties")
					.hdfs(syp.getApplicationDir() + "*.jar")
					.hdfs(syp.getApplicationDir() + "*.zip", LocalResourceType.ARCHIVE);
		}

		@Override
		public void configure(YarnEnvironmentConfigurer environment) throws Exception {
			environment
			.includeSystemEnv(true)
			.withClasspath()
				.entries(syap.getClasspath())
				.entry(explodedEntryIfZip(syap));
		}

		@Override
		public void configure(YarnAppmasterConfigurer master) throws Exception {
			master
				.appmasterClass(syap.getAppmasterClass() != null ? syap.getAppmasterClass() : appmasterClass)
				.containerCommands(createContainerCommands(syap));
		}

	}

	private static String explodedEntryIfZip(SpringYarnAppmasterProperties syap) {
		return StringUtils.endsWithIgnoreCase(syap.getContainerFile(), ".zip") ? "./" + syap.getContainerFile() : null;
	}

	private static String[] createContainerCommands(SpringYarnAppmasterProperties syap) throws Exception {
		LaunchCommandsFactoryBean factory = new LaunchCommandsFactoryBean();
		String containerJar = syap.getContainerFile();

		if (StringUtils.hasText(containerJar) && containerJar.endsWith("jar")) {
			factory.setJarFile(containerJar);
		} else if (StringUtils.hasText(syap.getContainerRunner())) {
			factory.setRunnerClass(syap.getContainerRunner());
		} else if (StringUtils.hasText(containerJar) && containerJar.endsWith("zip")) {
			factory.setRunnerClass("org.springframework.boot.loader.PropertiesLauncher");
		}

		if (syap.getArguments() != null) {
			Properties arguments = new Properties();
			arguments.putAll(syap.getArguments());
			factory.setArguments(arguments);
		}

		factory.setStdout("<LOG_DIR>/Container.stdout");
		factory.setStderr("<LOG_DIR>/Container.stderr");
		factory.afterPropertiesSet();
		return factory.getObject();
	}

}
