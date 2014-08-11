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
import java.util.Properties;
import java.util.Map.Entry;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.springframework.boot.cli.util.Log;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.app.YarnContainerClusterApplication;

/**
 * Command modifying a container cluster.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnClusterModifyCommand extends AbstractApplicationCommand {

	public YarnClusterModifyCommand() {
		super("clustermodify", "Modify cluster", new ClusterModifyOptionHandler());
	}

	private static final class ClusterModifyOptionHandler extends ApplicationOptionHandler {

		private OptionSpec<String> applicationIdOption;

		private OptionSpec<String> clusterIdOption;

		private OptionSpec<String> projectionDataAnyOption;

		private OptionSpec<String> projectionDataHostsOption;

		private OptionSpec<String> projectionDataRacksOption;

		@Override
		protected final void options() {
			this.applicationIdOption = option(CliSystemConstants.OPTIONS_APPLICATION_ID,
					CliSystemConstants.DESC_APPLICATION_ID).withRequiredArg();
			this.clusterIdOption = option(CliSystemConstants.OPTIONS_CLUSTER_ID, CliSystemConstants.DESC_CLUSTER_ID)
					.withRequiredArg();
			this.projectionDataAnyOption = option(CliSystemConstants.OPTIONS_PROJECTION_ANY,
					CliSystemConstants.DESC_PROJECTION_ANY).withRequiredArg();
			this.projectionDataHostsOption = option(CliSystemConstants.OPTIONS_PROJECTION_HOSTS,
					CliSystemConstants.DESC_PROJECTION_HOSTS).withOptionalArg().withValuesSeparatedBy(",");
			this.projectionDataRacksOption = option(CliSystemConstants.OPTIONS_PROJECTION_RACKS,
					CliSystemConstants.DESC_PROJECTION_RACKS).withOptionalArg().withValuesSeparatedBy(",");
		}

		@Override
		protected void runApplication(OptionSet options) throws Exception {
			String appId = options.valueOf(applicationIdOption);
			String clusterId = options.valueOf(clusterIdOption);
			String projectionAny = options.valueOf(projectionDataAnyOption);
			List<String> projectionHosts = options.valuesOf(projectionDataHostsOption);
			List<String> projectionRacks = options.valuesOf(projectionDataRacksOption);
			Assert.state(StringUtils.hasText(appId) && StringUtils.hasText(clusterId), "Cluster Id and Application Id must be defined");
			YarnContainerClusterApplication app = new YarnContainerClusterApplication();
			Properties appProperties = new Properties();
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.operation", "CLUSTERMODIFY");
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.applicationId",
					appId);
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.clusterId",
					clusterId);

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

			app.appProperties(appProperties);
			String info = app.run(new String[0]);
			Log.info(info);
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
