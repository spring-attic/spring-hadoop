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

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.hadoop.util.net.DefaultHostInfoDiscovery;
import org.springframework.data.hadoop.util.net.HostInfoDiscovery;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.StringUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.AppmasterTrackService;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.container.ContainerShutdown;
import org.springframework.yarn.am.grid.GridProjectionFactory;
import org.springframework.yarn.am.grid.GridProjectionFactoryLocator;
import org.springframework.yarn.am.grid.support.DefaultGridProjectionFactory;
import org.springframework.yarn.am.grid.support.GridProjectionFactoryRegistry;
import org.springframework.yarn.am.grid.support.ProjectionData;
import org.springframework.yarn.am.grid.support.ProjectionDataRegistry;
import org.springframework.yarn.batch.support.YarnJobLauncher;
import org.springframework.yarn.boot.actuate.endpoint.YarnContainerRegisterEndpoint;
import org.springframework.yarn.boot.actuate.endpoint.mvc.YarnContainerRegisterMvcEndpoint;
import org.springframework.yarn.boot.condition.ConditionalOnYarnAppmaster;
import org.springframework.yarn.boot.properties.SpringHadoopProperties;
import org.springframework.yarn.boot.properties.SpringYarnAppmasterLaunchContextProperties;
import org.springframework.yarn.boot.properties.SpringYarnAppmasterLocalizerProperties;
import org.springframework.yarn.boot.properties.SpringYarnAppmasterProperties;
import org.springframework.yarn.boot.properties.SpringYarnAppmasterProperties.ContainerClustersProjectionDataProperties;
import org.springframework.yarn.boot.properties.SpringYarnAppmasterProperties.ContainerClustersProjectionProperties;
import org.springframework.yarn.boot.properties.SpringYarnAppmasterProperties.ContainerClustersProperties;
import org.springframework.yarn.boot.properties.SpringYarnAppmasterResourceProperties;
import org.springframework.yarn.boot.properties.SpringYarnBatchProperties;
import org.springframework.yarn.boot.properties.SpringYarnEnvProperties;
import org.springframework.yarn.boot.properties.SpringYarnHostInfoDiscoveryProperties;
import org.springframework.yarn.boot.properties.SpringYarnProperties;
import org.springframework.yarn.boot.support.AppmasterLauncherRunner;
import org.springframework.yarn.boot.support.BootApplicationEventTransformer;
import org.springframework.yarn.boot.support.BootLocalResourcesSelector;
import org.springframework.yarn.boot.support.BootLocalResourcesSelector.Mode;
import org.springframework.yarn.boot.support.BootMultiLocalResourcesSelector;
import org.springframework.yarn.boot.support.EmbeddedAppmasterTrackService;
import org.springframework.yarn.boot.support.EndpointContainerShutdown;
import org.springframework.yarn.boot.support.SpringYarnBootUtils;
import org.springframework.yarn.config.annotation.EnableYarn;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnConfigConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerConfigurer;
import org.springframework.yarn.config.annotation.configurers.LocalResourcesHdfsConfigurer;
import org.springframework.yarn.config.annotation.configurers.MasterContainerAllocatorConfigurer;
import org.springframework.yarn.fs.LocalResourcesSelector;
import org.springframework.yarn.fs.LocalResourcesSelector.Entry;
import org.springframework.yarn.fs.MultiLocalResourcesSelector;
import org.springframework.yarn.launch.LaunchCommandsFactoryBean;
import org.springframework.yarn.support.ParsingUtils;
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
	@ConditionalOnWebApplication
	@EnableConfigurationProperties({ SpringYarnHostInfoDiscoveryProperties.class })
	public static class TrackServiceConfig {

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

		// if embedded servlet container exists we try to register
		// it as a track service with its address
		@Bean(name=YarnSystemConstants.DEFAULT_ID_AMTRACKSERVICE)
		@ConditionalOnMissingBean(AppmasterTrackService.class)
		public AppmasterTrackService appmasterTrackService(HostInfoDiscovery hostInfoDiscovery) {
			return new EmbeddedAppmasterTrackService(hostInfoDiscovery);
		}
	}

	@Configuration
	@EnableConfigurationProperties({ SpringYarnAppmasterProperties.class, SpringYarnAppmasterLocalizerProperties.class })
	public static class LocalResourcesSelectorConfig {

		@Autowired
		private SpringYarnAppmasterProperties syap;

		@Autowired
		private SpringYarnAppmasterLocalizerProperties syalp;

		@Bean
		@ConditionalOnMissingBean(LocalResourcesSelector.class)
		public LocalResourcesSelector localResourcesSelector() {

			Map<String, LocalResourcesSelector> selectors = new HashMap<String, LocalResourcesSelector>();
			if (syap.getContainercluster() != null && syap.getContainercluster().getClusters() != null) {
				for (java.util.Map.Entry<String, ContainerClustersProperties> entry : syap.getContainercluster().getClusters().entrySet()) {
					SpringYarnAppmasterLocalizerProperties props = entry.getValue().getLocalizer();
					if (props == null) {
						continue;
					}
					BootLocalResourcesSelector selector = new BootLocalResourcesSelector(Mode.CONTAINER);
					if (StringUtils.hasText(props.getZipPattern())) {
						selector.setZipArchivePattern(props.getZipPattern());
					}
					if (props.getPropertiesNames() != null) {
						selector.setPropertiesNames(props.getPropertiesNames());
					}
					if (props.getPropertiesSuffixes() != null) {
						selector.setPropertiesSuffixes(props.getPropertiesSuffixes());
					}
					selector.addPatterns(props.getPatterns());
					selectors.put(entry.getKey(), selector);
				}
			}

			BootLocalResourcesSelector selector = new BootLocalResourcesSelector(Mode.CONTAINER);
			if (StringUtils.hasText(syalp.getZipPattern())) {
				selector.setZipArchivePattern(syalp.getZipPattern());
			}
			if (syalp.getPropertiesNames() != null) {
				selector.setPropertiesNames(syalp.getPropertiesNames());
			}
			if (syalp.getPropertiesSuffixes() != null) {
				selector.setPropertiesSuffixes(syalp.getPropertiesSuffixes());
			}
			selector.addPatterns(syalp.getPatterns());
			return new BootMultiLocalResourcesSelector(selector, selectors);
		}
	}

	@Configuration
	@EnableConfigurationProperties({ SpringYarnAppmasterProperties.class })
	public static class RunnerConfig {

		@Autowired
		private SpringYarnAppmasterProperties syap;

		@Bean
		@ConditionalOnMissingBean(AppmasterLauncherRunner.class)
		@ConditionalOnBean(YarnAppmaster.class)
		public AppmasterLauncherRunner appmasterLauncherRunner() {
			AppmasterLauncherRunner runner = new AppmasterLauncherRunner();
			runner.setWaitLatch(syap.isKeepContextAlive());
			runner.setContainerCount(syap.getContainerCount());
			return runner;
		}
	}

	@Configuration
	@ConditionalOnClass(JobLauncher.class)
	@ConditionalOnExpression("${spring.yarn.batch.enabled:false}")
	@EnableConfigurationProperties({ SpringYarnBatchProperties.class })
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
		@ConditionalOnMissingBean(YarnJobLauncher.class)
		@ConditionalOnBean(JobLauncher.class)
		public YarnJobLauncher yarnJobLauncher() {
			YarnJobLauncher launcher = new YarnJobLauncher();
			if (StringUtils.hasText(jobName)) {
				launcher.setJobName(jobName);
			}
			return launcher;
		}

	}

	@Configuration
	@ConditionalOnExpression("${spring.yarn.appmaster.containercluster.enabled:false}")
	public static class ContainerClusterFactoryConfig  {

		@Bean
		@ConditionalOnMissingBean(name = "defaultGridProjectionFactory")
		public GridProjectionFactory defaultGridProjectionFactory() {
			// on its own @Configuration class because we
			// autowire in ContainerClusterConfig
			return new DefaultGridProjectionFactory();
		}

	}

	@Configuration
	@EnableConfigurationProperties({ SpringYarnAppmasterProperties.class })
	@ConditionalOnExpression("${spring.yarn.appmaster.containercluster.enabled:false}")
	public static class ContainerClusterConfig  {

		@Autowired
		private SpringYarnAppmasterProperties syap;

		@Autowired(required = false)
		private List<GridProjectionFactory> gridProjectionFactories;

		@Bean
		public GridProjectionFactoryLocator gridProjectionFactoryLocator() {
			GridProjectionFactoryRegistry registry = new GridProjectionFactoryRegistry();
			if (gridProjectionFactories != null) {
				for (GridProjectionFactory factory : gridProjectionFactories) {
					registry.addGridProjectionFactory(factory);
				}
			}
			return registry;
		}

		@Bean
		public ProjectionDataRegistry projectionDataRegistry() {
			Map<String, ProjectionData> projections = new HashMap<String, ProjectionData>();
			Map<String, ContainerClustersProperties> clusterProps = syap.getContainercluster().getClusters();
			if (clusterProps != null) {
				for (java.util.Map.Entry<String, ContainerClustersProperties> entry : clusterProps.entrySet()) {
					ProjectionData data = new ProjectionData();
					ContainerClustersProjectionProperties ccpProperties = entry.getValue().getProjection();
					if (ccpProperties != null) {
						ContainerClustersProjectionDataProperties ccpdProperties = ccpProperties.getData();
						if (ccpdProperties != null) {
							data.setAny(ccpdProperties.getAny());
							data.setHosts(ccpdProperties.getHosts());
							data.setRacks(ccpdProperties.getRacks());
							data.setProperties(ccpdProperties.getProperties());
						}
						data.setType(ccpProperties.getType());
					}

					SpringYarnAppmasterResourceProperties resource = entry.getValue().getResource();
					if (resource != null) {
						data.setPriority(resource.getPriority());
						data.setVirtualCores(resource.getVirtualCores());
						try {
							data.setMemory(ParsingUtils.parseBytesAsMegs(resource.getMemory()));
						} catch (ParseException e) {
						}
					}
					SpringYarnAppmasterLaunchContextProperties launchcontext = entry.getValue().getLaunchcontext();
					if (launchcontext != null) {
						data.setLocality(launchcontext.isLocality());
					}
					projections.put(entry.getKey(), data);

				}
			}
			return new ProjectionDataRegistry(projections);
		}

	}

	@Configuration
	@ConditionalOnExpression("${spring.yarn.endpoints.containerregister.enabled:false}")
	public static class ContainerRegisterEndPointConfig {

		@Bean
		@ConditionalOnMissingBean
		public YarnContainerRegisterEndpoint yarnContainerRegisterEndpoint() {
			return new YarnContainerRegisterEndpoint();
		}

		@Bean
		@ConditionalOnBean(YarnContainerRegisterEndpoint.class)
		public YarnContainerRegisterMvcEndpoint yarnContainerRegisterMvcEndpoint(YarnContainerRegisterEndpoint delegate) {
			return new YarnContainerRegisterMvcEndpoint(delegate);
		}

		@ConditionalOnExpression("${endpoints.shutdown.enabled:false}")
		@Bean(name=YarnSystemConstants.DEFAULT_CONTAINER_SHUTDOWN)
		public ContainerShutdown containerShutdown() {
			// only enable if boot shutdown is functional
			return new EndpointContainerShutdown();
		}

	}

	@Configuration
	@EnableConfigurationProperties({ SpringHadoopProperties.class, SpringYarnProperties.class,
			SpringYarnEnvProperties.class, SpringYarnAppmasterProperties.class,
			SpringYarnAppmasterLaunchContextProperties.class, SpringYarnAppmasterResourceProperties.class })
	@EnableYarn(enable=Enable.APPMASTER)
	static class Config extends SpringYarnConfigurerAdapter {

		@Autowired
		private SpringYarnProperties syp;

		@Autowired
		private SpringHadoopProperties shp;

		@Autowired
		private SpringYarnAppmasterProperties syap;

		@Autowired
		private SpringYarnAppmasterLaunchContextProperties syalcp;

		@Autowired
		private SpringYarnAppmasterResourceProperties syarp;

		@Autowired(required=false)
		@Qualifier("customAppmasterClass")
		private String appmasterClass;

		@Autowired
		private LocalResourcesSelector localResourcesSelector;

		@Override
		public void configure(YarnConfigConfigurer config) throws Exception {
			log.info("Configuring fsUri=[" + shp.getFsUri() + "]");
			log.info("Configuring rmAddress=[" + shp.getResourceManagerAddress() + "]");
			config
				.fileSystemUri(shp.getFsUri())
				.resourceManagerAddress(shp.getResourceManagerAddress())
				.schedulerAddress(shp.getResourceManagerSchedulerAddress())
				.withProperties()
					.properties(shp.getConfig())
					.and()
				.withResources()
					.resources(shp.getResources())
					.and()
				.withSecurity()
					.namenodePrincipal(shp.getSecurity() != null ? shp.getSecurity().getNamenodePrincipal() : null)
					.rmManagerPrincipal(shp.getSecurity() != null ? shp.getSecurity().getRmManagerPrincipal() : null)
					.authMethod(shp.getSecurity() != null ? shp.getSecurity().getAuthMethod() : null);
					// TODO: appmaster breaks if we try to use user principals
					//.userPrincipal(shp.getSecurity() != null ? shp.getSecurity().getUserPrincipal() : null)
					//.userKeytab(shp.getSecurity() != null ? shp.getSecurity().getUserKeytab() : null);
		}

		@Override
		public void configure(YarnResourceLocalizerConfigurer localizer) throws Exception {
			String applicationDir = SpringYarnBootUtils.resolveApplicationdir(syp);
			localizer
				.stagingDirectory(syp.getStagingDir());
			LocalResourcesHdfsConfigurer withHdfs = localizer.withHdfs();
			for (Entry e : localResourcesSelector.select(applicationDir != null ? applicationDir : "/")) {
				withHdfs.hdfs(e.getPath(), e.getType(), applicationDir == null);
			}

			if (syap.getContainercluster() != null && localResourcesSelector instanceof MultiLocalResourcesSelector && syap.getContainercluster().getClusters() != null) {
				MultiLocalResourcesSelector selector = ((MultiLocalResourcesSelector)localResourcesSelector);
				for (java.util.Map.Entry<String, ContainerClustersProperties> entry : syap.getContainercluster().getClusters().entrySet()) {
					withHdfs = localizer.withHdfs(entry.getKey());
					for (Entry e : selector.select(entry.getKey(), applicationDir != null ? applicationDir : "/")) {
						withHdfs.hdfs(e.getPath(), e.getType(), applicationDir == null);
					}
				}
			}
		}

		@Override
		public void configure(YarnEnvironmentConfigurer environment) throws Exception {
			environment
				.includeLocalSystemEnv(syalcp.isIncludeLocalSystemEnv())
				.withClasspath()
					.includeBaseDirectory(syalcp.isIncludeBaseDirectory())
					.useYarnAppClasspath(syalcp.isUseYarnAppClasspath())
					.useMapreduceAppClasspath(syalcp.isUseMapreduceAppClasspath())
					.siteYarnAppClasspath(syp.getSiteYarnAppClasspath())
					.siteMapreduceAppClasspath(syp.getSiteMapreduceAppClasspath())
					.delimiter(syalcp.getPathSeparator())
					.entries(syalcp.getContainerAppClasspath())
					.entry(explodedEntryIfZip(syalcp));

			if (syap.getContainercluster() != null && syap.getContainercluster().getClusters() != null) {
				for (java.util.Map.Entry<String, ContainerClustersProperties> entry : syap.getContainercluster().getClusters().entrySet()) {
					SpringYarnAppmasterLaunchContextProperties props = entry.getValue().getLaunchcontext();
					environment
						.withClasspath(entry.getKey())
							.includeBaseDirectory(props.isIncludeBaseDirectory())
							.useYarnAppClasspath(props.isUseYarnAppClasspath())
							.useMapreduceAppClasspath(props.isUseMapreduceAppClasspath())
							.siteYarnAppClasspath(syp.getSiteYarnAppClasspath())
							.siteMapreduceAppClasspath(syp.getSiteMapreduceAppClasspath())
							.delimiter(props.getPathSeparator())
							.entries(props.getContainerAppClasspath())
							.entry(explodedEntryIfZip(props));
				}
			}
		}

		@Override
		public void configure(YarnAppmasterConfigurer master) throws Exception {
			master
				.appmasterClass(syap.getAppmasterClass() != null ? syap.getAppmasterClass() : appmasterClass)
				.containerCommands(createContainerCommands(syalcp));

			MasterContainerAllocatorConfigurer containerAllocatorConfigurer = master.withContainerAllocator();
			containerAllocatorConfigurer
				.locality(syalcp.isLocality())
				.memory(syarp.getMemory())
				.priority(syarp.getPriority())
				.labelExpression(syarp.getLabelExpression())
				.virtualCores(syarp.getVirtualCores());

			if (syap.getContainercluster() != null && syap.getContainercluster().getClusters() != null) {
				for (java.util.Map.Entry<String, ContainerClustersProperties> entry : syap.getContainercluster().getClusters().entrySet()) {
					SpringYarnAppmasterResourceProperties resource = entry.getValue().getResource();
					SpringYarnAppmasterLaunchContextProperties launchcontext = entry.getValue().getLaunchcontext();

					master
						.containerCommands(entry.getKey(), createContainerCommands(launchcontext));

					containerAllocatorConfigurer
						.withCollection(entry.getKey())
							.priority(resource != null ? resource.getPriority() : null)
							.labelExpression(resource != null ? resource.getLabelExpression() : null)
							.memory(resource != null ? resource.getMemory() : null)
							.virtualCores(resource != null ? resource.getVirtualCores() : null)
							.locality(launchcontext != null ? launchcontext.isLocality() : false);
				}
			}
		}

	}

	private static String explodedEntryIfZip(SpringYarnAppmasterLaunchContextProperties syalcp) {
		return StringUtils.endsWithIgnoreCase(syalcp.getArchiveFile(), ".zip") ? "./" + syalcp.getArchiveFile() : null;
	}

	private static String[] createContainerCommands(SpringYarnAppmasterLaunchContextProperties syalcp) throws Exception {
		LaunchCommandsFactoryBean factory = new LaunchCommandsFactoryBean();
		String containerJar = syalcp.getArchiveFile();

		if (StringUtils.hasText(containerJar) && containerJar.endsWith("jar")) {
			factory.setJarFile(containerJar);
		} else if (StringUtils.hasText(syalcp.getRunnerClass())) {
			factory.setRunnerClass(syalcp.getRunnerClass());
		} else if (StringUtils.hasText(containerJar) && containerJar.endsWith("zip")) {
			factory.setRunnerClass("org.springframework.boot.loader.PropertiesLauncher");
		}

		factory.setArgumentsList(syalcp.getArgumentsList());

		if (syalcp.getArguments() != null) {
			Properties arguments = new Properties();
			arguments.putAll(syalcp.getArguments());
			factory.setArguments(arguments);
		}

		factory.setOptions(syalcp.getOptions());

		factory.setStdout("<LOG_DIR>/Container.stdout");
		factory.setStderr("<LOG_DIR>/Container.stderr");
		factory.afterPropertiesSet();
		return factory.getObject();
	}

}
