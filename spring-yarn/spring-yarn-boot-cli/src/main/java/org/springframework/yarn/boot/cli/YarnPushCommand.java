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

import org.springframework.yarn.boot.app.ClientApplicationRunner;
import org.springframework.yarn.boot.app.YarnPushApplication;

/**
 * Command pushing an application package into hdfs.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnPushCommand extends AbstractApplicationCommand {

	public final static String DEFAULT_COMMAND = "push";

	public final static String DEFAULT_DESC = "Push new application version";

	/**
	 * Instantiates a new yarn push command using a default
	 * command name, command description and option handler.
	 */
	public YarnPushCommand() {
		super(DEFAULT_COMMAND, DEFAULT_DESC, new PushOptionHandler());
	}

	/**
	 * Instantiates a new yarn push command  using a default
	 * command name and command description.
	 *
	 * @param handler the handler
	 */
	public YarnPushCommand(PushOptionHandler handler) {
		super(DEFAULT_COMMAND, DEFAULT_DESC, handler);
	}

	/**
	 * Instantiates a new yarn push command.
	 *
	 * @param name the command name
	 * @param description the command description
	 * @param handler the handler
	 */
	public YarnPushCommand(String name, String description, PushOptionHandler handler) {
		super(name, description, handler);
	}

	public static class PushOptionHandler extends ApplicationOptionHandler<String> {

		private OptionSpec<String> applicationVersionOption;

		@Override
		protected final void options() {
			this.applicationVersionOption = option(CliSystemConstants.OPTIONS_APPLICATION_VERSION,
					CliSystemConstants.DESC_APPLICATION_VERSION).withOptionalArg().defaultsTo("app");
		}

		@Override
		protected void runApplication(OptionSet options) throws Exception {
			String appVersion = options.valueOf(applicationVersionOption);
			YarnPushApplication app = new YarnPushApplication();
			app.applicationVersion(appVersion);
			Properties instanceProperties = new Properties();
			instanceProperties.setProperty("spring.yarn.applicationVersion", appVersion);
			app.configFile("application.properties", instanceProperties);
			handleApplicationRun(app);
		}

		@Override
		protected void handleApplicationRun(ClientApplicationRunner<String> app) {
			app.run();
			handleOutput("New version installed");
		}

		public OptionSpec<String> getApplicationVersionOption() {
			return applicationVersionOption;
		}

	}

}
