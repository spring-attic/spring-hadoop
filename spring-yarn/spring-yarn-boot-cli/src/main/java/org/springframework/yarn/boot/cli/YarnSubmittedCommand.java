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

import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.app.YarnInfoApplication;

/**
 * Command listing submitted applications.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnSubmittedCommand extends AbstractApplicationCommand {

	public final static String DEFAULT_COMMAND = "submitted";

	public final static String DEFAULT_DESC = "List submitted applications";

	/**
	 * Instantiates a new yarn submitted command using a default
	 * command name, command description and option handler.
	 */
	public YarnSubmittedCommand() {
		this(DEFAULT_COMMAND, DEFAULT_DESC, new SubmittedOptionHandler(null));
	}

	/**
	 * Instantiates a new yarn submitted command  using a default
	 * command name and command description.
	 *
	 * @param handler the handler
	 */
	public YarnSubmittedCommand(SubmittedOptionHandler handler) {
		this(DEFAULT_COMMAND, DEFAULT_DESC, handler);
	}

	/**
	 * Instantiates a new yarn submitted command.
	 *
	 * @param name the command name
	 * @param description the command description
	 * @param handler the handler
	 */
	public YarnSubmittedCommand(String name, String description, SubmittedOptionHandler handler) {
		super(name, description, handler);
	}

	public static class SubmittedOptionHandler extends ApplicationOptionHandler<String> {

		private String defaultAppType = "BOOT";

		private OptionSpec<String> typeOption;

		private OptionSpec<Boolean> verboseOption;

		public SubmittedOptionHandler() {
			this(null);
		}

		public SubmittedOptionHandler(String defaultAppType) {
			if (StringUtils.hasText(defaultAppType)) {
				this.defaultAppType = defaultAppType;
			}
		}

		@Override
		protected final void options() {
			typeOption = option(CliSystemConstants.OPTIONS_APPLICATION_TYPE, CliSystemConstants.DESC_APPLICATION_TYPE).withOptionalArg().defaultsTo(defaultAppType);
			verboseOption = option(CliSystemConstants.OPTIONS_VERBOSE, CliSystemConstants.DESC_VERBOSE).withOptionalArg().ofType(Boolean.class)
					.defaultsTo(true);
		}

		@Override
		protected void runApplication(OptionSet options) throws Exception {
			YarnInfoApplication app = new YarnInfoApplication();
			Properties appProperties = new Properties();
			appProperties.setProperty("spring.yarn.internal.yarn-info-application.operation", "SUBMITTED");
			if (isFlagOn(options, verboseOption)) {
				appProperties.setProperty("spring.yarn.internal.yarn-info-application.verbose", "true");
			}
			appProperties.setProperty("spring.yarn.internal.yarn-info-application.type", options.valueOf(typeOption));
			app.appProperties(appProperties);
			handleApplicationRun(app);
		}

		public String getDefaultAppType() {
			return defaultAppType;
		}

		public OptionSpec<String> getTypeOption() {
			return typeOption;
		}

		public OptionSpec<Boolean> getVerboseOption() {
			return verboseOption;
		}

	}

}
