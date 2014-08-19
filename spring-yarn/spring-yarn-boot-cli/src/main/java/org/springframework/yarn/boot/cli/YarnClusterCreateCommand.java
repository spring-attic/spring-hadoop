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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

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

	public static class ClusterCreateOptionHandler extends ApplicationOptionHandler {

		private OptionSpec<String> applicationIdOption;

		private OptionSpec<String> clusterIdOption;

		private OptionSpec<String> clusterDefOption;

		private OptionSpec<String> projectionTypeOption;

		private OptionSpec<String> projectionDataAnyOption;

		private OptionSpec<String> projectionDataHostsOption;

		private OptionSpec<String> projectionDataRacksOption;

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
					CliSystemConstants.DESC_PROJECTION_HOSTS).withOptionalArg().withValuesSeparatedBy(",");
			this.projectionDataRacksOption = option(CliSystemConstants.OPTIONS_PROJECTION_RACKS,
					CliSystemConstants.DESC_PROJECTION_RACKS).withOptionalArg().withValuesSeparatedBy(",");
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
			List<String> projectionHosts = options.valuesOf(projectionDataHostsOption);
			List<String> projectionRacks = options.valuesOf(projectionDataRacksOption);

			YarnContainerClusterApplication app = new YarnContainerClusterApplication();
			Properties appProperties = new Properties();
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.operation", "CLUSTERCREATE");
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.applicationId",
					appId);
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.clusterId",
					clusterId);
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.clusterDef",
					clusterDef);
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.projectionType",
					projectionType);

			if (StringUtils.hasText(projectionAny)) {
				appProperties
						.setProperty("spring.yarn.internal.ContainerClusterApplication.projectionDataAny", projectionAny);
			}

			for (Entry<String, Integer> entry : getMapFromString(projectionHosts).entrySet()) {
				appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.projectionDataHosts."
						+ entry.getKey(), entry.getValue().toString());
			}

			for (Entry<String, Integer> entry : getMapFromString(projectionRacks).entrySet()) {
				appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.projectionDataRacks."
						+ entry.getKey(), entry.getValue().toString());
			}

			Properties extraProperties = getExtraProperties(options);
			if (extraProperties != null) {
				for (String key : extraProperties.stringPropertyNames()) {
					appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.extraProperties."
							+ key, extraProperties.getProperty(key));
				}
			}

			app.appProperties(appProperties);
			handleOutput(app.run());
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

		private static Map<String, Integer> getMapFromString(List<String> sources) {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			if (sources != null) {
				String currentHost = null;
				for (String source : sources) {
					if (isNumber(source)) {
						map.put(currentHost, Integer.parseInt(source));
					} else {
						currentHost = source;
					}
					if (!map.containsKey(currentHost)) {
						map.put(currentHost, 1);
					} else {
					}
				}
			}
			return map;
		}

		private static boolean isNumber(String source) {
			try {
				Integer.parseInt(source);
				return true;
			} catch (NumberFormatException e) {
			}
			return false;
		}

	}

}
