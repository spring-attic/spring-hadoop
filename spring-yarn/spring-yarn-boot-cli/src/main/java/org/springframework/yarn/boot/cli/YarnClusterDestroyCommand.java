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

import java.util.Properties;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.app.YarnContainerClusterApplication;

/**
 * Command destroying a container cluster.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnClusterDestroyCommand extends AbstractApplicationCommand {

	public final static String DEFAULT_COMMAND = "clusterdestroy";

	public final static String DEFAULT_DESC = "Destroy cluster";

	/**
	 * Instantiates a new yarn cluster destroy command using a default
	 * command name, command description and option handler.
	 */
	public YarnClusterDestroyCommand() {
		super(DEFAULT_COMMAND, DEFAULT_DESC, new ClusterDestroyOptionHandler());
	}

	/**
	 * Instantiates a new yarn cluster destroy command using a default
	 * command name and command description.
	 *
	 * @param handler the handler
	 */
	public YarnClusterDestroyCommand(ClusterDestroyOptionHandler handler) {
		super(DEFAULT_COMMAND, DEFAULT_DESC, handler);
	}

	/**
	 * Instantiates a new yarn cluster destroy command.
	 *
	 * @param name the command name
	 * @param description the command description
	 * @param handler the handler
	 */
	public YarnClusterDestroyCommand(String name, String description, ClusterDestroyOptionHandler handler) {
		super(name, description, handler);
	}

	public static class ClusterDestroyOptionHandler extends ApplicationOptionHandler<String> {

		private OptionSpec<String> applicationIdOption;

		private OptionSpec<String> clusterIdOption;

		@Override
		protected final void options() {
			this.applicationIdOption = option(CliSystemConstants.OPTIONS_APPLICATION_ID,
					CliSystemConstants.DESC_APPLICATION_ID).withRequiredArg();
			this.clusterIdOption = option(CliSystemConstants.OPTIONS_CLUSTER_ID, CliSystemConstants.DESC_CLUSTER_ID)
					.withRequiredArg();
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
			String versionId = options.valueOf(clusterIdOption);
			YarnContainerClusterApplication app = new YarnContainerClusterApplication();
			Properties appProperties = new Properties();
			appProperties.setProperty("spring.yarn.internal.container-cluster-application.operation", "CLUSTERDESTROY");
			appProperties.setProperty("spring.yarn.internal.container-cluster-application.applicationId",
					appId);
			appProperties.setProperty("spring.yarn.internal.container-cluster-application.clusterId",
					versionId);
			app.appProperties(appProperties);
			handleApplicationRun(app);
		}

		public OptionSpec<String> getApplicationIdOption() {
			return applicationIdOption;
		}

		public OptionSpec<String> getClusterIdOption() {
			return clusterIdOption;
		}

	}

}
