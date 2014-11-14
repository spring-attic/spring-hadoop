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
package org.springframework.yarn.boot.cli;

import java.util.Map.Entry;
import java.util.Properties;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.app.YarnContainerClusterApplication;

/**
 * Command creating a container cluster.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnClusterCreateCommand extends AbstractApplicationCommand {

	public final static String DEFAULT_COMMAND = "clustercreate";

	public final static String DEFAULT_DESC = "Create cluster";

	/**
	 * Instantiates a new yarn cluster create command using a default
	 * command name, command description and option handler.
	 */
	public YarnClusterCreateCommand() {
		super(DEFAULT_COMMAND, DEFAULT_DESC, new ClusterCreateOptionHandler());
	}

	/**
	 * Instantiates a new yarn cluster create command using a default
	 * command name and command description.
	 *
	 * @param handler the handler
	 */
	public YarnClusterCreateCommand(ClusterCreateOptionHandler handler) {
		super(DEFAULT_COMMAND, DEFAULT_DESC, handler);
	}

	/**
	 * Instantiates a new yarn cluster create command.
	 *
	 * @param name the command name
	 * @param description the command description
	 * @param handler the handler
	 */
	public YarnClusterCreateCommand(String name, String description, ClusterCreateOptionHandler handler) {
		super(name, description, handler);
	}

	public static class ClusterCreateOptionHandler extends ApplicationOptionHandler<String> {

		private static final String PREFIX = "spring.yarn.internal.ContainerClusterApplication";

		private OptionSpec<String> applicationIdOption;

		private OptionSpec<String> clusterIdOption;

		private OptionSpec<String> clusterDefOption;

		private OptionSpec<String> projectionTypeOption;

		private OptionSpec<String> projectionDataAnyOption;

		private OptionSpec<String> projectionDataHostsOption;

		private OptionSpec<String> projectionDataRacksOption;

		private OptionSpec<String> projectionDataRawOption;

		@Override
		protected void options() {
			this.applicationIdOption = option(CliSystemConstants.OPTIONS_APPLICATION_ID,
					CliSystemConstants.DESC_APPLICATION_ID).withRequiredArg();
			this.clusterIdOption = option(CliSystemConstants.OPTIONS_CLUSTER_ID, CliSystemConstants.DESC_CLUSTER_ID)
					.withRequiredArg();
			this.clusterDefOption = option(CliSystemConstants.OPTIONS_CLUSTER_DEF, CliSystemConstants.DESC_CLUSTER_DEF)
					.withOptionalArg();
			this.projectionTypeOption = option(CliSystemConstants.OPTIONS_PROJECTION_TYPE,
					CliSystemConstants.DESC_PROJECTION_TYPE).withRequiredArg();
			this.projectionDataAnyOption = option(CliSystemConstants.OPTIONS_PROJECTION_ANY,
					CliSystemConstants.DESC_PROJECTION_ANY).withRequiredArg();
			this.projectionDataHostsOption = option(CliSystemConstants.OPTIONS_PROJECTION_HOSTS,
					CliSystemConstants.DESC_PROJECTION_HOSTS).withOptionalArg();
			this.projectionDataRacksOption = option(CliSystemConstants.OPTIONS_PROJECTION_RACKS,
					CliSystemConstants.DESC_PROJECTION_RACKS).withOptionalArg();
			this.projectionDataRawOption = option(CliSystemConstants.OPTIONS_PROJECTION_DATA,
					CliSystemConstants.DESC_PROJECTION_DATA).withOptionalArg();
		}

		@Override
		protected void verifyOptionSet(OptionSet options) throws Exception {
			String appId = options.valueOf(applicationIdOption);
			String clusterId = options.valueOf(clusterIdOption);
			Assert.state(StringUtils.hasText(appId) && StringUtils.hasText(clusterId), "Cluster Id and Application Id must be defined");
		}

		@Override
		protected void runApplication(OptionSet options) throws Exception {
			String appId = options.valueOf(applicationIdOption);
			String clusterId = options.valueOf(clusterIdOption);
			String clusterDef = options.valueOf(clusterDefOption);
			String projectionType = options.valueOf(projectionTypeOption);
			String projectionAny = options.valueOf(projectionDataAnyOption);
			String projectionHosts = options.valueOf(projectionDataHostsOption);
			String projectionRacks = options.valueOf(projectionDataRacksOption);
			String projectionRaw = options.valueOf(projectionDataRawOption);

			YarnContainerClusterApplication app = new YarnContainerClusterApplication();
			Properties appProperties = new Properties();
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.operation", "CLUSTERCREATE");
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.applicationId",
					appId);
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.clusterId",
					clusterId);
			if (clusterDef != null) {
				appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.clusterDef", clusterDef);
			}
			if (projectionType != null) {
				appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.projectionType",
						projectionType);
			}

			if (StringUtils.hasText(projectionAny)) {
				appProperties.setProperty(PREFIX + ".projectionData.any", projectionAny);
			}

			if (StringUtils.hasText(projectionHosts)) {
				for (Entry<Object, Object> entry : getPropertiesFromRawYaml(projectionHosts).entrySet()) {
					appProperties.setProperty(PREFIX + ".projectionData.hosts." + entry.getKey(), entry.getValue().toString());
				}
			}

			if (StringUtils.hasText(projectionRacks)) {
				for (Entry<Object, Object> entry : getPropertiesFromRawYaml(projectionRacks).entrySet()) {
					appProperties.setProperty(PREFIX + ".projectionData.racks." + entry.getKey(), entry.getValue().toString());
				}
			}

			if (StringUtils.hasText(projectionRaw)) {
				for (Entry<Object, Object> entry : getPropertiesFromRawYaml(projectionRaw).entrySet()) {
					appProperties.setProperty(PREFIX + ".projectionData." + entry.getKey(), entry.getValue().toString());
				}
			}

			Properties extraProperties = getExtraProperties(options);
			if (extraProperties != null) {
				for (String key : extraProperties.stringPropertyNames()) {
					appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.extraProperties."
							+ key, extraProperties.getProperty(key));
				}
			}

			app.appProperties(appProperties);
			handleApplicationRun(app);
		}

		public OptionSpec<String> getApplicationIdOption() {
			return applicationIdOption;
		}

		public OptionSpec<String> getClusterIdOption() {
			return clusterIdOption;
		}

		public OptionSpec<String> getClusterDefOption() {
			return clusterDefOption;
		}

		public OptionSpec<String> getProjectionTypeOption() {
			return projectionTypeOption;
		}

		public OptionSpec<String> getProjectionDataAnyOption() {
			return projectionDataAnyOption;
		}

		public OptionSpec<String> getProjectionDataHostsOption() {
			return projectionDataHostsOption;
		}

		public OptionSpec<String> getProjectionDataRacksOption() {
			return projectionDataRacksOption;
		}

		protected Properties getExtraProperties(OptionSet options) {
			return null;
		}

		private static Properties getPropertiesFromRawYaml(String raw) {
			if (StringUtils.isEmpty(raw)) {
				return new Properties();
			}
			Resource resource = new ByteArrayResource(raw.getBytes());
			YamlPropertiesFactoryBean ypfb = new YamlPropertiesFactoryBean();
			ypfb.setResources(resource);
			ypfb.afterPropertiesSet();
			return ypfb.getObject();
		}

	}

}
