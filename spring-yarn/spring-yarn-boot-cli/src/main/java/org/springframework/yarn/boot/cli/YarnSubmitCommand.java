/*
 * Copyright 2014-2015 the original author or authors.
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

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.app.ClientApplicationRunner;
import org.springframework.yarn.boot.app.YarnSubmitApplication;

/**
 * Command submitting a new application instance.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnSubmitCommand extends AbstractApplicationCommand {

	public final static String DEFAULT_COMMAND = "submit";

	public final static String DEFAULT_DESC = "Submit application";

	/**
	 * Instantiates a new yarn submit command using a default
	 * command name, command description and option handler.
	 */
	public YarnSubmitCommand() {
		super(DEFAULT_COMMAND, DEFAULT_DESC, new SubmitOptionHandler());
	}

	/**
	 * Instantiates a new yarn submit command  using a default
	 * command name and command description.
	 *
	 * @param handler the handler
	 */
	public YarnSubmitCommand(SubmitOptionHandler handler) {
		super(DEFAULT_COMMAND, DEFAULT_DESC, handler);
	}

	/**
	 * Instantiates a new yarn submit command.
	 *
	 * @param name the command name
	 * @param description the command description
	 * @param handler the handler
	 */
	public YarnSubmitCommand(String name, String description, SubmitOptionHandler handler) {
		super(name, description, handler);
	}

	public static class SubmitOptionHandler extends ApplicationOptionHandler<ApplicationId> {

		private OptionSpec<String> applicationVersionOption;

		private OptionSpec<String> applicationNameOption;
		
		@Override
		protected final void options() {
			this.applicationVersionOption = option(CliSystemConstants.OPTIONS_APPLICATION_VERSION,
					CliSystemConstants.DESC_APPLICATION_VERSION).withOptionalArg().defaultsTo("app");
			this.applicationNameOption = option(CliSystemConstants.OPTIONS_APPLICATION_NAME,
					CliSystemConstants.DESC_APPLICATION_NAME).withOptionalArg();
		}

		@Override
		protected void runApplication(OptionSet options) throws Exception {
			String appVersion = options.valueOf(applicationVersionOption);
			String appName = options.valueOf(applicationNameOption);
			Assert.hasText(appVersion, "Application version must be defined");
			YarnSubmitApplication app = new YarnSubmitApplication();
			if (StringUtils.hasText(appName)) {
				app.applicationName(appName);
			}
			app.applicationVersion(appVersion);
			handleApplicationRun(app);
		}

		@Override
		protected void handleApplicationRun(ClientApplicationRunner<ApplicationId> app) {
			ApplicationId applicationId = app.run();
			handleOutput("New instance submitted with id " + applicationId);
		}

		/**
		 * Get the application version option.
		 * 
		 * @return the application version option
		 */
		public OptionSpec<String> getApplicationVersionOption() {
			return applicationVersionOption;
		}
		
		/**
		 * Get the application name option.
		 * 
		 * @return the application name option
		 */
		public OptionSpec<String> getApplicationNameOption() {
			return applicationNameOption;
		}		

	}

}
