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

import org.junit.Test;
import org.springframework.yarn.boot.cli.YarnShutdownCommand.ShutdownOptionHandler;

/**
 * Tests for {@link YarnShutdownCommand}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnShutdownCommandTests {

	@Test
	public void testDefaultOptionHelp() {
		YarnShutdownCommand command = new YarnShutdownCommand();
		assertThat(command.getHelp(), containsString("-a, --application-id"));
	}

	@Test
	public void testShouldNotFail() throws Exception {
		NoRunShutdownOptionHandler handler = new NoRunShutdownOptionHandler();
		YarnShutdownCommand command = new YarnShutdownCommand(handler);
		command.run("-a", "foo");
		assertThat(handler.appId, is("foo"));
	}

	private static class NoRunShutdownOptionHandler extends ShutdownOptionHandler {

		String appId;

		@Override
		protected void runApplication(OptionSet options) throws Exception {
			appId = options.valueOf(getApplicationIdOption());
		}

	}

}
