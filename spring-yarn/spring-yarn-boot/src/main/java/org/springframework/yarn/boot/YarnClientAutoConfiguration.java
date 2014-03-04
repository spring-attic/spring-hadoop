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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.condition.ConditionalOnMissingYarn;
import org.springframework.yarn.boot.properties.SpringHadoopProperties;
import org.springframework.yarn.boot.properties.SpringYarnClientLaunchContextProperties;
import org.springframework.yarn.boot.properties.SpringYarnClientLocalizerProperties;
import org.springframework.yarn.boot.properties.SpringYarnClientProperties;
import org.springframework.yarn.boot.properties.SpringYarnClientResourceProperties;
import org.springframework.yarn.boot.properties.SpringYarnEnvProperties;
import org.springframework.yarn.boot.properties.SpringYarnProperties;
import org.springframework.yarn.boot.support.BootLocalResourcesSelector;
import org.springframework.yarn.boot.support.BootLocalResourcesSelector.Mode;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.config.annotation.EnableYarn;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.builders.YarnClientConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnConfigConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerConfigurer;
import org.springframework.yarn.config.annotation.configurers.LocalResourcesHdfsConfigurer;
import org.springframework.yarn.fs.LocalResourcesSelector;
import org.springframework.yarn.fs.LocalResourcesSelector.Entry;
import org.springframework.yarn.launch.LaunchCommandsFactoryBean;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Spring Hadoop Yarn
 * {@link YarnClient}.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@ConditionalOnMissingYarn
@ConditionalOnClass(EnableYarn.class)
@ConditionalOnMissingBean(YarnClient.class)
public class YarnClientAutoConfiguration {

	@Configuration
	@EnableConfigurationProperties({ SpringYarnClientLocalizerProperties.class })
	public static class LocalResourcesSelectorConfig {

		@Autowired
		private SpringYarnClientLocalizerProperties syclp;

		@Bean
		@ConditionalOnMissingBean(LocalResourcesSelector.class)
		public LocalResourcesSelector localResourcesSelector() {
			BootLocalResourcesSelector selector = new BootLocalResourcesSelector(Mode.APPMASTER);
			if (StringUtils.hasText(syclp.getZipPattern())) {
				selector.setZipArchivePattern(syclp.getZipPattern());
			}
			if (syclp.getPropertiesNames() != null) {
				selector.setPropertiesNames(syclp.getPropertiesNames());
			}
			if (syclp.getPropertiesSuffixes() != null) {
				selector.setPropertiesSuffixes(syclp.getPropertiesSuffixes());
			}
			selector.addPatterns(syclp.getPatterns());
			return selector;
		}
	}

	@Configuration
	@EnableConfigurationProperties({ SpringHadoopProperties.class, SpringYarnProperties.class,
			SpringYarnEnvProperties.class, SpringYarnClientProperties.class,
			SpringYarnClientLaunchContextProperties.class, SpringYarnClientLocalizerProperties.class,
			SpringYarnClientResourceProperties.class })
	@EnableYarn(enable=Enable.CLIENT)
	public static class SpringYarnConfig extends SpringYarnConfigurerAdapter {

		@Autowired
		private SpringYarnProperties syp;

		@Autowired
		private SpringHadoopProperties shp;

		@Autowired
		private SpringYarnClientProperties sycp;

		@Autowired
		private SpringYarnClientLaunchContextProperties syclcp;

		@Autowired
		private SpringYarnClientLocalizerProperties syclp;

		@Autowired
		private SpringYarnClientResourceProperties sycrp;

		@Autowired
		private LocalResourcesSelector localResourcesSelector;

		@Override
		public void configure(YarnConfigConfigurer config) throws Exception {
			config
				.fileSystemUri(shp.getFsUri())
				.resourceManagerAddress(shp.getResourceManagerAddress())
				.schedulerAddress(shp.getResourceManagerSchedulerAddress());
		}

		@Override
		public void configure(YarnResourceLocalizerConfigurer localizer) throws Exception {
			localizer
				.stagingDirectory(syp.getStagingDir())
				.withCopy()
					.copy(StringUtils.toStringArray(sycp.getFiles()), syp.getApplicationDir(), syp.getApplicationDir() == null)
					.raw(syclp.getRawFileContents(), syp.getApplicationDir());

			LocalResourcesHdfsConfigurer withHdfs = localizer.withHdfs();
			for (Entry e : localResourcesSelector.select(syp.getApplicationDir() != null ? syp.getApplicationDir() : "/")) {
				withHdfs.hdfs(e.getPath(), e.getType(), syp.getApplicationDir() == null);
			}
		}

		@Override
		public void configure(YarnEnvironmentConfigurer environment) throws Exception {
			environment
				.includeSystemEnv(syclcp.isIncludeSystemEnv())
				.withClasspath()
					.includeBaseDirectory(syclcp.isIncludeBaseDirectory())
					.useDefaultYarnClasspath(syclcp.isUseDefaultYarnClasspath())
					.defaultYarnAppClasspath(syp.getDefaultYarnAppClasspath())
					.delimiter(syclcp.getPathSeparator())
					.entries(syclcp.getClasspath())
					.entry(explodedEntryIfZip(syclcp));
		}

		@Override
		public void configure(YarnClientConfigurer client) throws Exception {
			client
				.appName(syp.getAppName())
				.appType(syp.getAppType())
				.priority(sycp.getPriority())
				.queue(sycp.getQueue())
				.memory(sycrp.getMemory())
				.virtualCores(sycrp.getVirtualCores())
				.masterCommands(createMasterCommands(syclcp));
		}

	}

	private static String explodedEntryIfZip(SpringYarnClientLaunchContextProperties syclcp) {
		return StringUtils.endsWithIgnoreCase(syclcp.getArchiveFile(), ".zip") ? "./" + syclcp.getArchiveFile() : null;
	}

	/**
	 * Builds a raw command set used to start application master.
	 */
	private static String[] createMasterCommands(SpringYarnClientLaunchContextProperties syclcp) throws Exception {
		LaunchCommandsFactoryBean factory = new LaunchCommandsFactoryBean();
		String appmasterJar = syclcp.getArchiveFile();

		if (StringUtils.hasText(appmasterJar) && appmasterJar.endsWith("jar")) {
			factory.setJarFile(syclcp.getArchiveFile());
		} else if (StringUtils.hasText(syclcp.getRunnerClass())) {
			factory.setRunnerClass(syclcp.getRunnerClass());
		} else if (StringUtils.hasText(appmasterJar) && appmasterJar.endsWith("zip")) {
			factory.setRunnerClass("org.springframework.boot.loader.PropertiesLauncher");
		}

		if (syclcp.getArguments() != null) {
			Properties arguments = new Properties();
			arguments.putAll(syclcp.getArguments());
			factory.setArguments(arguments);
		}

		factory.setOptions(syclcp.getOptions());

		factory.setStdout("<LOG_DIR>/Appmaster.stdout");
		factory.setStderr("<LOG_DIR>/Appmaster.stderr");
		factory.afterPropertiesSet();
		return factory.getObject();
	}

}
