/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.yarn.boot.cli;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import joptsimple.OptionSet;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.yarn.boot.app.ClientApplicationRunner;
import org.springframework.yarn.boot.cli.YarnKillCommand.KillOptionHandler;

/**
 * Tests for {@link YarnKillCommand}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnKillCommandTests {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testDefaultOptionHelp() {
		YarnKillCommand command = new YarnKillCommand();
		assertThat(command.getHelp(), containsString("-a, --application-id"));
	}

	@Test
	public void testShouldNotFail() throws Exception {
		NoRunKillOptionHandler handler = new NoRunKillOptionHandler();
		YarnKillCommand command = new YarnKillCommand(handler);
		command.run("-a", "foo");
		assertThat(handler.appId, is("foo"));
	}

	@Test
	public void testCustomOptionHandlerFailure() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage(is("no jee"));
		CustomKillOptionHandler handler = new CustomKillOptionHandler();
		YarnKillCommand command = new YarnKillCommand(handler);
		command.run("-a", "foo");
	}

	@Test
	public void testRequiredArgs() throws Exception {
		NoHandleApplicationOptionHandler handler = new NoHandleApplicationOptionHandler();
		YarnKillCommand command = new YarnKillCommand(handler);
		command.run("-a", "aaa");
		assertThat(handler.output, is("output"));
	}

	private static class CustomKillOptionHandler extends KillOptionHandler {

		@Override
		protected void verifyOptionSet(OptionSet options) throws Exception {
			String appId = options.valueOf(getApplicationIdOption());
			if (!appId.startsWith("jee")) {
				throw new IllegalArgumentException("no jee");
			}
		}

	}

	private static class NoRunKillOptionHandler extends KillOptionHandler {

		String appId;

		@Override
		protected void runApplication(OptionSet options) throws Exception {
			appId = options.valueOf(getApplicationIdOption());
		}

	}

	private static class NoHandleApplicationOptionHandler extends KillOptionHandler {

		String output;

		@Override
		protected void handleApplicationRun(ClientApplicationRunner<String> app) {
			this.output = "output";
		}

	}

}
