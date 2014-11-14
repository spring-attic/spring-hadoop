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
import org.springframework.yarn.boot.cli.YarnClusterCreateCommand.ClusterCreateOptionHandler;

/**
 * Tests for {@link YarnClusterCreateCommand}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnClusterCreateCommandTests {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testDefaultOptionHelp() {
		YarnClusterCreateCommand command = new YarnClusterCreateCommand();
		assertThat(command.getHelp(), containsString("-a, --application-id"));
		assertThat(command.getHelp(), containsString("-c, --cluster-id"));
		assertThat(command.getHelp(), containsString("-h, --projection-hosts"));
		assertThat(command.getHelp(), containsString("-i, --cluster-def"));
		assertThat(command.getHelp(), containsString("-p, --projection-type"));
		assertThat(command.getHelp(), containsString("-r, --projection-racks"));
		assertThat(command.getHelp(), containsString("-w, --projection-any"));
		assertThat(command.getHelp(), containsString("-y, --projection-data"));
	}

	@Test
	public void testFailureNoArgs() throws Exception {
		thrown.expect(IllegalStateException.class);
		thrown.expectMessage(containsString("Cluster Id and Application Id must be defined"));
		NoRunApplicationClusterCreateOptionHandler handler = new NoRunApplicationClusterCreateOptionHandler();
		YarnClusterCreateCommand command = new YarnClusterCreateCommand(handler);
		command.run(new String[0]);
	}

	@Test
	public void testShouldNotFail() throws Exception {
		NoRunApplicationClusterCreateOptionHandler handler = new NoRunApplicationClusterCreateOptionHandler();
		YarnClusterCreateCommand command = new YarnClusterCreateCommand(handler);
		command.run("-a", "xxx", "-c", "xxx");
	}

	@Test
	public void testCustomOptionHandlerFailure() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage(is("no jee"));
		CustomClusterCreateOptionHandler handler = new CustomClusterCreateOptionHandler();
		YarnClusterCreateCommand command = new YarnClusterCreateCommand(handler);
		command.run("-a", "foo");
	}

	@Test
	public void testMissingClusterDef() throws Exception {
		NoHandleApplicationOptionHandler handler = new NoHandleApplicationOptionHandler();
		YarnClusterCreateCommand command = new YarnClusterCreateCommand(handler);
		command.run("-a", "aaa", "-c", "ccc");
		assertThat(handler.output, is("output"));
	}

	private static class CustomClusterCreateOptionHandler extends ClusterCreateOptionHandler {

		@Override
		protected void verifyOptionSet(OptionSet options) throws Exception {
			String appId = options.valueOf(getApplicationIdOption());
			if (!appId.startsWith("jee")) {
				throw new IllegalArgumentException("no jee");
			}
		}

	}

	private static class NoRunApplicationClusterCreateOptionHandler extends ClusterCreateOptionHandler {

		@Override
		protected void runApplication(OptionSet options) throws Exception {
		}

	}

	private static class NoHandleApplicationOptionHandler extends ClusterCreateOptionHandler {

		String output;

		@Override
		protected void handleApplicationRun(ClientApplicationRunner<String> app) {
			this.output = "output";
		}

	}

}
