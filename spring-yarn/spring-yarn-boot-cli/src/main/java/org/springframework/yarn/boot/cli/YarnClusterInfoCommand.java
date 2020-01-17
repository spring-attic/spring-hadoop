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
 * Command listing info about a container cluster.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnClusterInfoCommand extends AbstractApplicationCommand {

	public final static String DEFAULT_COMMAND = "clusterinfo";

	public final static String DEFAULT_DESC = "List cluster info";

	/**
	 * Instantiates a new yarn cluster info command using a default
	 * command name, command description and option handler.
	 */
	public YarnClusterInfoCommand() {
		super(DEFAULT_COMMAND, DEFAULT_DESC, new ClusterInfoOptionHandler());
	}

	/**
	 * Instantiates a new yarn cluster info command using a default
	 * command name and command description.
	 *
	 * @param handler the handler
	 */
	public YarnClusterInfoCommand(ClusterInfoOptionHandler handler) {
		super(DEFAULT_COMMAND, DEFAULT_DESC, handler);
	}

	/**
	 * Instantiates a new yarn cluster info command.
	 *
	 * @param name the command name
	 * @param description the command description
	 * @param handler the handler
	 */
	public YarnClusterInfoCommand(String name, String description, ClusterInfoOptionHandler handler) {
		super(name, description, handler);
	}

	public static class ClusterInfoOptionHandler extends ApplicationOptionHandler<String> {

		private OptionSpec<String> applicationIdOption;

		private OptionSpec<String> clusterIdOption;

		private OptionSpec<Boolean> verboseOption;

		@Override
		protected final void options() {
			applicationIdOption = option(CliSystemConstants.OPTIONS_APPLICATION_ID, CliSystemConstants.DESC_APPLICATION_ID).withRequiredArg();
			clusterIdOption = option(CliSystemConstants.OPTIONS_CLUSTER_ID, CliSystemConstants.DESC_CLUSTER_ID).withRequiredArg();
			verboseOption = option(CliSystemConstants.OPTIONS_VERBOSE, CliSystemConstants.DESC_VERBOSE).withOptionalArg().ofType(Boolean.class)
					.defaultsTo(true);
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
			YarnContainerClusterApplication app = new YarnContainerClusterApplication();
			Properties appProperties = new Properties();
			appProperties.setProperty("spring.yarn.internal.container-cluster-application.operation", "CLUSTERINFO");
			appProperties.setProperty("spring.yarn.internal.container-cluster-application.applicationId",
					appId);
			appProperties.setProperty("spring.yarn.internal.container-cluster-application.clusterId",
					clusterId);
			if (isFlagOn(options, verboseOption)) {
				appProperties.setProperty("spring.yarn.internal.container-cluster-application.verbose", "true");
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

		public OptionSpec<Boolean> getVerboseOption() {
			return verboseOption;
		}

	}

}
