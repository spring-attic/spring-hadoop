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
import org.springframework.yarn.boot.app.YarnKillApplication;

/**
 * Command killing yarn applications.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnKillCommand extends AbstractApplicationCommand {

	public final static String DEFAULT_COMMAND = "kill";

	public final static String DEFAULT_DESC = "Kill application";

	/**
	 * Instantiates a new yarn kill command using a default command name,
	 * command description and option handler.
	 */
	public YarnKillCommand() {
		super(DEFAULT_COMMAND, DEFAULT_DESC, new KillOptionHandler());
	}

	/**
	 * Instantiates a new yarn kill command using a default command name and
	 * command description.
	 *
	 * @param handler the handler
	 */
	public YarnKillCommand(KillOptionHandler handler) {
		super(DEFAULT_COMMAND, DEFAULT_DESC, handler);
	}

	/**
	 * Instantiates a new yarn kill command.
	 *
	 * @param name the command name
	 * @param description the command description
	 * @param handler the handler
	 */
	public YarnKillCommand(String name, String description, KillOptionHandler handler) {
		super(name, description, handler);
	}

	public static class KillOptionHandler extends ApplicationOptionHandler<String> {

		private OptionSpec<String> applicationIdOption;

		@Override
		protected final void options() {
			this.applicationIdOption = option(CliSystemConstants.OPTIONS_APPLICATION_ID,
					CliSystemConstants.DESC_APPLICATION_ID).withRequiredArg();
		}

		@Override
		protected void runApplication(OptionSet options) throws Exception {
			String appId = options.valueOf(applicationIdOption);
			Assert.hasText(appId, "Application Id must be defined");
			YarnKillApplication app = new YarnKillApplication();
			Properties appProperties = new Properties();
			appProperties.setProperty("spring.yarn.internal.yarn-kill-application.applicationId", appId);
			app.appProperties(appProperties);
			handleApplicationRun(app);
		}

		public OptionSpec<String> getApplicationIdOption() {
			return applicationIdOption;
		}

	}

}
