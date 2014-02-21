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
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.boot.condition.ConditionalOnYarnAppmaster;
import org.springframework.yarn.boot.support.AppmasterLauncherRunner;
import org.springframework.yarn.boot.support.BootApplicationEventTransformer;
import org.springframework.yarn.boot.support.BootLocalResourcesSelector;
import org.springframework.yarn.boot.support.BootLocalResourcesSelector.Mode;
import org.springframework.yarn.boot.support.SpringYarnAppmasterProperties;
import org.springframework.yarn.boot.support.SpringYarnBatchProperties;
import org.springframework.yarn.boot.support.SpringYarnEnvProperties;
import org.springframework.yarn.boot.support.SpringYarnProperties;
import org.springframework.yarn.boot.support.YarnJobLauncherCommandLineRunner;
import org.springframework.yarn.config.annotation.EnableYarn;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnConfigConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerConfigurer;
import org.springframework.yarn.config.annotation.configurers.LocalResourcesHdfsConfigurer;
import org.springframework.yarn.fs.LocalResourcesSelector;
import org.springframework.yarn.fs.LocalResourcesSelector.Entry;
import org.springframework.yarn.launch.LaunchCommandsFactoryBean;
import org.springframework.yarn.support.YarnContextUtils;

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
	public static class LocalResourcesSelectorConfig {

		@Autowired
		private SpringYarnAppmasterProperties syap;

		@Bean
		@ConditionalOnMissingBean(LocalResourcesSelector.class)
		public LocalResourcesSelector localResourcesSelector() {
			BootLocalResourcesSelector selector = new BootLocalResourcesSelector(Mode.CONTAINER);
			if (StringUtils.hasText(syap.getLocalizerZipPattern())) {
				selector.setZipArchivePattern(syap.getLocalizerZipPattern());
			}
			if (syap.getLocalizerPropertiesNames() != null) {
				selector.setPropertiesNames(syap.getLocalizerPropertiesNames());
			}
			if (syap.getLocalizerPropertiesSuffixes() != null) {
				selector.setPropertiesSuffixes(syap.getLocalizerPropertiesSuffixes());
			}
			selector.addPatterns(syap.getLocalizerPatterns());
			return selector;
		}
	}

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
	@ConditionalOnExpression("${spring.yarn.batch.enabled:false}")
	@EnableConfigurationProperties({SpringYarnBatchProperties.class})
	public static class RuntimeConfig {

		@Value("${spring.yarn.batch.name:}")
		private String jobName;

		@Bean(name=YarnContextUtils.TASK_EXECUTOR_BEAN_NAME)
		public TaskExecutor threadPoolTaskExecutor() {
			ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
			executor.setCorePoolSize(2);
			return executor;
		}

		@Bean
		public String customAppmasterClass() {
			// class reference would fail if not in classpath
			return "org.springframework.yarn.batch.am.BatchAppmaster";
		}

		@Bean
		public BootApplicationEventTransformer bootApplicationEventTransformer() {
			return new BootApplicationEventTransformer();
		}

		@Bean
		@ConditionalOnMissingBean(YarnJobLauncherCommandLineRunner.class)
		@ConditionalOnBean(JobLauncher.class)
		public YarnJobLauncherCommandLineRunner jobLauncherCommandLineRunner() {
			YarnJobLauncherCommandLineRunner runner = new YarnJobLauncherCommandLineRunner();
			if (StringUtils.hasText(jobName)) {
				runner.setJobName(jobName);
			}
			return runner;
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

		@Autowired
		private LocalResourcesSelector localResourcesSelector;

		@Override
		public void configure(YarnConfigConfigurer config) throws Exception {
			log.info("Configuring fsUri=[" + syp.getFsUri() + "]");
			log.info("Configuring rmAddress=[" + syp.getRmAddress() + "]");
			config
				.fileSystemUri(syp.getFsUri())
				.resourceManagerAddress(syp.getRmAddress())
				.schedulerAddress(syp.getSchedulerAddress());
		}

		@Override
		public void configure(YarnResourceLocalizerConfigurer localizer) throws Exception {
			LocalResourcesHdfsConfigurer withHdfs = localizer.withHdfs();
			for (Entry e : localResourcesSelector.select(syp.getApplicationDir())) {
				withHdfs.hdfs(e.getPath(), e.getType());
			}
		}

		@Override
		public void configure(YarnEnvironmentConfigurer environment) throws Exception {
			environment
				.includeSystemEnv(syap.isIncludeSystemEnv())
				.withClasspath()
					.includeBaseDirectory(syap.isIncludeBaseDirectory())
					.defaultYarnAppClasspath(syap.isDefaultYarnAppClasspath())
					.delimiter(syap.getDelimiter())
					.entries(syap.getClasspath())
					.entry(explodedEntryIfZip(syap));
		}

		@Override
		public void configure(YarnAppmasterConfigurer master) throws Exception {
			master
				.appmasterClass(syap.getAppmasterClass() != null ? syap.getAppmasterClass() : appmasterClass)
				.containerCommands(createContainerCommands(syap))
				.withContainerAllocator()
					.memory(syap.getMemory())
					.priority(syap.getPriority())
					.virtualCores(syap.getVirtualCores());
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

		factory.setOptions(syap.getOptions());

		factory.setStdout("<LOG_DIR>/Container.stdout");
		factory.setStderr("<LOG_DIR>/Container.stderr");
		factory.afterPropertiesSet();
		return factory.getObject();
	}

}
