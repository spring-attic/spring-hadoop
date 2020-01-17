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

import org.springframework.yarn.boot.app.YarnInfoApplication;

/**
 * Command listing pushed application from hdfs.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnPushedCommand extends AbstractApplicationCommand {

	public final static String DEFAULT_COMMAND = "pushed";

	public final static String DEFAULT_DESC = "List pushed applications";

	/**
	 * Instantiates a new yarn pushed command using a default
	 * command name, command description and option handler.
	 */
	public YarnPushedCommand() {
		super(DEFAULT_COMMAND, DEFAULT_DESC, new PushedOptionHandler());
	}

	/**
	 * Instantiates a new yarn pushed command using a default
	 * command name and command description.
	 *
	 * @param handler the handler
	 */
	public YarnPushedCommand(PushedOptionHandler handler) {
		super(DEFAULT_COMMAND, DEFAULT_DESC, handler);
	}

	/**
	 * Instantiates a new yarn pushed command.
	 *
	 * @param name the command name
	 * @param description the command description
	 * @param handler the handler
	 */
	public YarnPushedCommand(String name, String description, PushedOptionHandler handler) {
		super(name, description, handler);
	}

	public static class PushedOptionHandler extends ApplicationOptionHandler<String> {

		@Override
		protected void runApplication(OptionSet options) throws Exception {
			YarnInfoApplication app = new YarnInfoApplication();
			Properties appProperties = new Properties();
			appProperties.setProperty("spring.yarn.internal.yarn-info-application.operation", "PUSHED");
			app.appProperties(appProperties);
			handleApplicationRun(app);
		}

	}

}
