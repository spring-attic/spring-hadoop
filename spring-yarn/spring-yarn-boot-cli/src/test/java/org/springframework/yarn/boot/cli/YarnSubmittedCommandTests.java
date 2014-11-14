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
import org.springframework.yarn.boot.cli.YarnSubmittedCommand.SubmittedOptionHandler;

/**
 * Tests for {@link YarnSubmittedCommand}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnSubmittedCommandTests {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testDefaultOptionHelp() {
		YarnSubmittedCommand command = new YarnSubmittedCommand();
		assertThat(command.getHelp(), containsString("-t, --application-type"));
		assertThat(command.getHelp(), containsString("-v, --verbose"));
	}

	@Test
	public void testShouldNotFail() throws Exception {
		NoRunSubmittedOptionHandler handler = new NoRunSubmittedOptionHandler(null);
		YarnSubmittedCommand command = new YarnSubmittedCommand(handler);
		command.run();
		assertThat(handler.getDefaultAppType(), is("BOOT"));
		assertThat(handler.type, is("BOOT"));
		assertThat(handler.verbose, is(false));
	}

	@Test
	public void testArgs() throws Exception {
		NoRunSubmittedOptionHandler handler = new NoRunSubmittedOptionHandler(null);
		YarnSubmittedCommand command = new YarnSubmittedCommand(handler);
		command.run("-t", "GS", "-v");
		assertThat(handler.getDefaultAppType(), is("BOOT"));
		assertThat(handler.type, is("GS"));
		assertThat(handler.verbose, is(true));
	}

	@Test
	public void testChangeDefaultAppType() throws Exception {
		NoRunSubmittedOptionHandler handler = new NoRunSubmittedOptionHandler("GS");
		YarnSubmittedCommand command = new YarnSubmittedCommand(handler);
		command.run();
		assertThat(handler.getDefaultAppType(), is("GS"));
	}

	@Test
	public void testCustomOptionHandlerFailure() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage(is("no jee"));
		CustomSubmittedOptionHandler handler = new CustomSubmittedOptionHandler(null);
		YarnSubmittedCommand command = new YarnSubmittedCommand(handler);
		command.run();
	}

	@Test
	public void testRequiredArgs() throws Exception {
		NoHandleApplicationOptionHandler handler = new NoHandleApplicationOptionHandler();
		YarnSubmittedCommand command = new YarnSubmittedCommand(handler);
		command.run();
		assertThat(handler.output, is("output"));
	}

	private static class CustomSubmittedOptionHandler extends SubmittedOptionHandler {

		public CustomSubmittedOptionHandler(String defaultAppType) {
			super(defaultAppType);
		}

		@Override
		protected void verifyOptionSet(OptionSet options) throws Exception {
			throw new IllegalArgumentException("no jee");
		}

	}

	private static class NoRunSubmittedOptionHandler extends SubmittedOptionHandler {

		String type;
		Boolean verbose;

		public NoRunSubmittedOptionHandler(String defaultAppType) {
			super(defaultAppType);
		}

		@Override
		protected void verifyOptionSet(OptionSet options) throws Exception {
			type = options.valueOf(getTypeOption());
			verbose = isFlagOn(options, getVerboseOption());
		}

		@Override
		protected void runApplication(OptionSet options) throws Exception {
		}

	}

	private static class NoHandleApplicationOptionHandler extends SubmittedOptionHandler {

		String output;

		@Override
		protected void handleApplicationRun(ClientApplicationRunner<String> app) {
			this.output = "output";
		}

	}

}
