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

import org.springframework.boot.cli.util.Log;
import org.springframework.util.Assert;
import org.springframework.yarn.boot.app.YarnContainerClusterApplication;

/**
 * Command listing info about container clusters.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnClustersInfoCommand extends AbstractApplicationCommand {

	public YarnClustersInfoCommand() {
		super("clustersinfo", "List clusters", new ClustersInfoOptionHandler());
	}

	private static final class ClustersInfoOptionHandler extends ApplicationOptionHandler {

		private OptionSpec<String> applicationIdOption;

		@Override
		protected final void options() {
			this.applicationIdOption = option(CliSystemConstants.OPTIONS_APPLICATION_ID, CliSystemConstants.DESC_APPLICATION_ID).withRequiredArg();
		}

		@Override
		protected void runApplication(OptionSet options) throws Exception {
			String appId = options.valueOf(applicationIdOption);
			Assert.hasText(appId, "Application Id must be defined");
			YarnContainerClusterApplication app = new YarnContainerClusterApplication();
			Properties appProperties = new Properties();
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.operation", "CLUSTERSINFO");
			appProperties.setProperty("spring.yarn.internal.ContainerClusterApplication.applicationId",
					appId);
			app.appProperties(appProperties);
			String info = app.run(new String[0]);
			Log.info(info);
		}

	}

}
