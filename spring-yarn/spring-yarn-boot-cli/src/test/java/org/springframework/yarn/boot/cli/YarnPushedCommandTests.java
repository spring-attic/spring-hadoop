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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import joptsimple.OptionSet;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.yarn.boot.app.ClientApplicationRunner;
import org.springframework.yarn.boot.cli.YarnPushedCommand.PushedOptionHandler;

/**
 * Tests for {@link YarnPushedCommand}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnPushedCommandTests {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testDefaultOptionHelp() {
		YarnPushedCommand command = new YarnPushedCommand();
		assertThat(command.getHelp(), containsString("No options specified"));
	}

	@Test
	public void testShouldNotFail() throws Exception {
		NoRunClusterCreateOptionHandler handler = new NoRunClusterCreateOptionHandler();
		YarnPushedCommand command = new YarnPushedCommand(handler);
		command.run();
	}

	@Test
	public void testCustomOptionHandlerFailure() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage(is("no jee"));
		CustomPushedOptionHandler handler = new CustomPushedOptionHandler();
		YarnPushedCommand command = new YarnPushedCommand(handler);
		command.run();
	}

	@Test
	public void testRequiredArgs() throws Exception {
		NoHandleApplicationOptionHandler handler = new NoHandleApplicationOptionHandler();
		YarnPushedCommand command = new YarnPushedCommand(handler);
		command.run();
		assertThat(handler.output, is("output"));
	}

	private static class CustomPushedOptionHandler extends PushedOptionHandler {

		@Override
		protected void verifyOptionSet(OptionSet options) throws Exception {
			throw new IllegalArgumentException("no jee");
		}

	}

	private static class NoRunClusterCreateOptionHandler extends PushedOptionHandler {

		@Override
		protected void runApplication(OptionSet options) throws Exception {
		}

	}

	private static class NoHandleApplicationOptionHandler extends PushedOptionHandler {

		String output;

		@Override
		protected void handleApplicationRun(ClientApplicationRunner<String> app) {
			this.output = "output";
		}

	}

}
