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

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.springframework.boot.cli.command.OptionParsingCommand;
import org.springframework.boot.cli.command.options.OptionHandler;
import org.springframework.boot.cli.command.status.ExitStatus;
import org.springframework.boot.cli.util.Log;
import org.springframework.yarn.boot.app.ClientApplicationRunner;

/**
 * Base class for all commands implemented by
 * this cli package.
 *
 * @author Janne Valkealahti
 *
 */
public class AbstractApplicationCommand extends OptionParsingCommand {

	/**
	 * Instantiates a new abstract application command.
	 *
	 * @param name the command name
	 * @param description the command description
	 * @param handler the option handler
	 */
	protected AbstractApplicationCommand(String name, String description, OptionHandler handler) {
		super(name, description, handler);
	}

	public abstract static class ApplicationOptionHandler<R> extends OptionHandler {

		@Override
		protected final ExitStatus run(OptionSet options) throws Exception {
			verifyOptionSet(options);
			runApplication(options);
			return ExitStatus.OK;
		}

		/**
		 * Verify option before application is executed. This method
		 * is called before {@link #runApplication(OptionSet)}.
		 *
		 * @param options the options set
		 * @throws Exception if error occurred during the processing
		 */
		protected void verifyOptionSet(OptionSet options) throws Exception {
		}

		/**
		 * Run the application.
		 *
		 * @param options the options set
		 * @throws Exception if error occurred during the processing
		 */
		protected abstract void runApplication(OptionSet options) throws Exception;

		/**
		 * Utility method to check if boolean flag is set.
		 *
		 * @param options the options set
		 * @param option the boolean option spec
		 * @return true if boolean option is enabled
		 */
		protected boolean isFlagOn(OptionSet options, OptionSpec<Boolean> option) {
			return options.has(option) ? options.valueOf(option) : false;
		}

		/**
		 * Utility method to handle output for the command.
		 * Default implementation simply logs using boot
		 * cli {@link Log}.
		 *
		 * @param output command output
		 */
		protected void handleOutput(String output) {
			Log.info(output);
		}

		/**
		 * Handles run of {@link ClientApplicationRunner}.
		 *
		 * @param app the app
		 */
		protected void handleApplicationRun(ClientApplicationRunner<R> app) {
			handleApplicationRun(app, new String[0]);
		}

		/**
		 * Handles run of {@link ClientApplicationRunner}.
		 *
		 * @param app the app
		 * @param args the args
		 */
		protected void handleApplicationRun(ClientApplicationRunner<R> app, String... args) {
			R run = app.run(args);
			if (run != null) {
				handleOutput(run.toString());
			}
		}

	}

	@Override
	public String getUsageHelp() {
		return "[options]";
	}

}
