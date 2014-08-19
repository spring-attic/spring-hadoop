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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class CliTester implements TestRule {

	private final OutputCapture outputCapture = new OutputCapture();

	public void run(AbstractCli cli, String... args) {
		cli.doMain(args);
	}

	public String getOutput() {
		return this.outputCapture.toString();
	}

	@Override
	public Statement apply(Statement base, Description description) {
		final Statement statement = CliTester.this.outputCapture.apply(new RunLauncherStatement(base), description);
		return new Statement() {

			@Override
			public void evaluate() throws Throwable {
				statement.evaluate();
			}
		};
	}

	private final class RunLauncherStatement extends Statement {

		private final Statement base;

		private RunLauncherStatement(Statement base) {
			this.base = base;
		}

		@Override
		public void evaluate() throws Throwable {
			this.base.evaluate();
		}

	}

}
