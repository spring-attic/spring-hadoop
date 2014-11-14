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
import org.springframework.yarn.boot.cli.YarnClusterInfoCommand.ClusterInfoOptionHandler;

/**
 * Tests for {@link YarnClusterInfoCommand}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnClusterInfoCommandTests {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testDefaultOptionHelp() {
		YarnClusterInfoCommand command = new YarnClusterInfoCommand();
		assertThat(command.getHelp(), containsString("-a, --application-id"));
		assertThat(command.getHelp(), containsString("-c, --cluster-id"));
		assertThat(command.getHelp(), containsString("-v, --verbose"));
	}

	@Test
	public void testFailureNoArgs() throws Exception {
		thrown.expect(IllegalStateException.class);
		thrown.expectMessage(containsString("Cluster Id and Application Id must be defined"));
		NoRunClusterInfoOptionHandler handler = new NoRunClusterInfoOptionHandler();
		YarnClusterInfoCommand command = new YarnClusterInfoCommand(handler);
		command.run(new String[0]);
	}

	@Test
	public void testShouldNotFail() throws Exception {
		NoRunClusterInfoOptionHandler handler = new NoRunClusterInfoOptionHandler();
		YarnClusterInfoCommand command = new YarnClusterInfoCommand(handler);
		command.run("-a", "xxx", "-c", "xxx", "-v");
	}

	@Test
	public void testCustomOptionHandlerFailure() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage(is("no jee"));
		CustomClusterInfoOptionHandler handler = new CustomClusterInfoOptionHandler();
		YarnClusterInfoCommand command = new YarnClusterInfoCommand(handler);
		command.run("-a", "foo");
	}

	@Test
	public void testRequiredArgs() throws Exception {
		NoHandleApplicationOptionHandler handler = new NoHandleApplicationOptionHandler();
		YarnClusterInfoCommand command = new YarnClusterInfoCommand(handler);
		command.run("-a", "aaa", "-c", "ccc");
		assertThat(handler.output, is("output"));
	}

	private static class CustomClusterInfoOptionHandler extends ClusterInfoOptionHandler {

		@Override
		protected void verifyOptionSet(OptionSet options) throws Exception {
			String appId = options.valueOf(getApplicationIdOption());
			if (!appId.startsWith("jee")) {
				throw new IllegalArgumentException("no jee");
			}
		}

	}

	private static class NoRunClusterInfoOptionHandler extends ClusterInfoOptionHandler {

		@Override
		protected void runApplication(OptionSet options) throws Exception {
		}

	}

	private static class NoHandleApplicationOptionHandler extends ClusterInfoOptionHandler {

		String output;

		@Override
		protected void handleApplicationRun(ClientApplicationRunner<String> app) {
			this.output = "output";
		}

	}

}
