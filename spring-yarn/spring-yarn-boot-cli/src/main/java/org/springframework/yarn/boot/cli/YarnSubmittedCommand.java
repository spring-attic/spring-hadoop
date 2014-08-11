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
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.app.YarnInfoApplication;

/**
 * Command listing submitted applications.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnSubmittedCommand extends AbstractApplicationCommand {

	public YarnSubmittedCommand() {
		this(null);
	}

	public YarnSubmittedCommand(String defaultAppType) {
		super("submitted", "List submitted applications", new SubmittedOptionHandler(defaultAppType));
	}

	private static final class SubmittedOptionHandler extends ApplicationOptionHandler {

		private String defaultAppType = "BOOT";

		private OptionSpec<String> typeOption;

		private OptionSpec<Boolean> verboseOption;

		private SubmittedOptionHandler(String defaultAppType) {
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
			appProperties.setProperty("spring.yarn.internal.YarnInfoApplication.operation", "SUBMITTED");
			if (isFlagOn(options, verboseOption)) {
				appProperties.setProperty("spring.yarn.internal.YarnInfoApplication.verbose", "true");
			}
			appProperties.setProperty("spring.yarn.internal.YarnInfoApplication.type", options.valueOf(typeOption));
			app.appProperties(appProperties);
			String info = app.run(new String[0]);
			Log.info(info);
		}

	}

}
