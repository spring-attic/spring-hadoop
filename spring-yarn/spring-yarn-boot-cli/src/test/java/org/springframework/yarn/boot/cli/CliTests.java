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
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.springframework.boot.cli.command.Command;
import org.springframework.boot.cli.command.CommandRunner;
import org.springframework.boot.cli.command.HelpExample;
import org.springframework.boot.cli.command.options.OptionHelp;
import org.springframework.boot.cli.command.status.ExitStatus;
import org.springframework.boot.cli.util.Log;
import org.springframework.yarn.boot.cli.YarnClusterCreateCommand.ClusterCreateOptionHandler;
import org.springframework.yarn.boot.cli.YarnClusterDestroyCommand.ClusterDestroyOptionHandler;
import org.springframework.yarn.boot.cli.YarnClusterInfoCommand.ClusterInfoOptionHandler;
import org.springframework.yarn.boot.cli.YarnClusterModifyCommand.ClusterModifyOptionHandler;
import org.springframework.yarn.boot.cli.YarnClusterStartCommand.ClusterStartOptionHandler;
import org.springframework.yarn.boot.cli.YarnClusterStopCommand.ClusterStopOptionHandler;
import org.springframework.yarn.boot.cli.YarnClustersInfoCommand.ClustersInfoOptionHandler;
import org.springframework.yarn.boot.cli.YarnKillCommand.KillOptionHandler;
import org.springframework.yarn.boot.cli.YarnPushCommand.PushOptionHandler;
import org.springframework.yarn.boot.cli.YarnPushedCommand.PushedOptionHandler;
import org.springframework.yarn.boot.cli.YarnSubmitCommand.SubmitOptionHandler;
import org.springframework.yarn.boot.cli.YarnSubmittedCommand.SubmittedOptionHandler;
import org.springframework.yarn.boot.cli.shell.ShellCommand;

/**
 * Tests for cli integration.
 *
 * @author Janne Valkealahti
 *
 */
public class CliTests {

	@Rule
	public CliTester tester = new CliTester();

	@Test
	public void testNoArgs() {
		TestCli cli = new TestCli();
		tester.run(cli, new String[0]);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(1));
		assertThat(output, containsString(YarnPushCommand.DEFAULT_COMMAND));
		assertThat(output, containsString(YarnPushCommand.DEFAULT_DESC));
		assertThat(output, containsString(YarnPushedCommand.DEFAULT_COMMAND));
		assertThat(output, containsString(YarnPushedCommand.DEFAULT_DESC));
		assertThat(output, containsString(YarnSubmitCommand.DEFAULT_COMMAND));
		assertThat(output, containsString(YarnSubmitCommand.DEFAULT_DESC));
		assertThat(output, containsString(YarnSubmittedCommand.DEFAULT_COMMAND));
		assertThat(output, containsString(YarnSubmittedCommand.DEFAULT_DESC));
		assertThat(output, containsString(YarnKillCommand.DEFAULT_COMMAND));
		assertThat(output, containsString(YarnKillCommand.DEFAULT_DESC));
		assertThat(output, containsString(YarnClustersInfoCommand.DEFAULT_COMMAND));
		assertThat(output, containsString(YarnClustersInfoCommand.DEFAULT_DESC));
		assertThat(output, containsString(YarnClusterInfoCommand.DEFAULT_COMMAND));
		assertThat(output, containsString(YarnClusterInfoCommand.DEFAULT_DESC));
		assertThat(output, containsString(YarnClusterCreateCommand.DEFAULT_COMMAND));
		assertThat(output, containsString(YarnClusterCreateCommand.DEFAULT_DESC));
		assertThat(output, containsString(YarnClusterStartCommand.DEFAULT_COMMAND));
		assertThat(output, containsString(YarnClusterStartCommand.DEFAULT_DESC));
		assertThat(output, containsString(YarnClusterStopCommand.DEFAULT_COMMAND));
		assertThat(output, containsString(YarnClusterStopCommand.DEFAULT_DESC));
		assertThat(output, containsString(YarnClusterModifyCommand.DEFAULT_COMMAND));
		assertThat(output, containsString(YarnClusterModifyCommand.DEFAULT_DESC));
		assertThat(output, containsString(YarnClusterDestroyCommand.DEFAULT_COMMAND));
		assertThat(output, containsString(YarnClusterDestroyCommand.DEFAULT_DESC));
	}

	@Test
	public void testNoArgsChangeCommandAndDesc() {
		TestCliCustomCommands cli = new TestCliCustomCommands();
		tester.run(cli, new String[0]);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(1));
		assertThat(output, containsString(YarnPushCommand.DEFAULT_COMMAND + "x"));
		assertThat(output, containsString(YarnPushCommand.DEFAULT_DESC + "x"));
		assertThat(output, containsString(YarnPushedCommand.DEFAULT_COMMAND + "x"));
		assertThat(output, containsString(YarnPushedCommand.DEFAULT_DESC + "x"));
		assertThat(output, containsString(YarnSubmitCommand.DEFAULT_COMMAND + "x"));
		assertThat(output, containsString(YarnSubmitCommand.DEFAULT_DESC + "x"));
		assertThat(output, containsString(YarnSubmittedCommand.DEFAULT_COMMAND + "x"));
		assertThat(output, containsString(YarnSubmittedCommand.DEFAULT_DESC + "x"));
		assertThat(output, containsString(YarnKillCommand.DEFAULT_COMMAND + "x"));
		assertThat(output, containsString(YarnKillCommand.DEFAULT_DESC + "x"));
		assertThat(output, containsString(YarnClustersInfoCommand.DEFAULT_COMMAND + "x"));
		assertThat(output, containsString(YarnClustersInfoCommand.DEFAULT_DESC + "x"));
		assertThat(output, containsString(YarnClusterInfoCommand.DEFAULT_COMMAND + "x"));
		assertThat(output, containsString(YarnClusterInfoCommand.DEFAULT_DESC + "x"));
		assertThat(output, containsString(YarnClusterCreateCommand.DEFAULT_COMMAND + "x"));
		assertThat(output, containsString(YarnClusterCreateCommand.DEFAULT_DESC + "x"));
		assertThat(output, containsString(YarnClusterStartCommand.DEFAULT_COMMAND + "x"));
		assertThat(output, containsString(YarnClusterStartCommand.DEFAULT_DESC + "x"));
		assertThat(output, containsString(YarnClusterStopCommand.DEFAULT_COMMAND + "x"));
		assertThat(output, containsString(YarnClusterStopCommand.DEFAULT_DESC + "x"));
		assertThat(output, containsString(YarnClusterModifyCommand.DEFAULT_COMMAND + "x"));
		assertThat(output, containsString(YarnClusterModifyCommand.DEFAULT_DESC + "x"));
		assertThat(output, containsString(YarnClusterDestroyCommand.DEFAULT_COMMAND + "x"));
		assertThat(output, containsString(YarnClusterDestroyCommand.DEFAULT_DESC + "x"));
	}

	@Test
	public void testYarnPush() {
		TestCli cli = new TestCli();
		tester.run(cli, "help", YarnPushCommand.DEFAULT_COMMAND);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString(YarnPushCommand.DEFAULT_COMMAND + " - " + YarnPushCommand.DEFAULT_DESC));
	}

	@Test
	public void testYarnPushed() {
		TestCli cli = new TestCli();
		tester.run(cli, "help", YarnPushedCommand.DEFAULT_COMMAND);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString(YarnPushedCommand.DEFAULT_COMMAND + " - " + YarnPushedCommand.DEFAULT_DESC));
	}

	@Test
	public void testYarnSubmit() {
		TestCli cli = new TestCli();
		tester.run(cli, "help", YarnSubmitCommand.DEFAULT_COMMAND);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString(YarnSubmitCommand.DEFAULT_COMMAND + " - " + YarnSubmitCommand.DEFAULT_DESC));
	}

	@Test
	public void testYarnSubmitted() {
		TestCli cli = new TestCli();
		tester.run(cli, "help", YarnSubmittedCommand.DEFAULT_COMMAND);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString(YarnSubmittedCommand.DEFAULT_COMMAND + " - " + YarnSubmittedCommand.DEFAULT_DESC));
	}

	@Test
	public void testYarnKill() {
		TestCli cli = new TestCli();
		tester.run(cli, "help", YarnKillCommand.DEFAULT_COMMAND);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString(YarnKillCommand.DEFAULT_COMMAND + " - " + YarnKillCommand.DEFAULT_DESC));
	}

	@Test
	public void testHelpClustersinfo() {
		TestCli cli = new TestCli();
		tester.run(cli, "help", YarnClustersInfoCommand.DEFAULT_COMMAND);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString(YarnClustersInfoCommand.DEFAULT_COMMAND + " - " + YarnClustersInfoCommand.DEFAULT_DESC));
	}

	@Test
	public void testHelpClusterinfo() {
		TestCli cli = new TestCli();
		tester.run(cli, "help", YarnClusterInfoCommand.DEFAULT_COMMAND);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString(YarnClusterInfoCommand.DEFAULT_COMMAND + " - " + YarnClusterInfoCommand.DEFAULT_DESC));
	}

	@Test
	public void testHelpClustercreate() {
		TestCli cli = new TestCli();
		tester.run(cli, "help", YarnClusterCreateCommand.DEFAULT_COMMAND);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString(YarnClusterCreateCommand.DEFAULT_COMMAND + " - " + YarnClusterCreateCommand.DEFAULT_DESC));
	}

	@Test
	public void testHelpClusterstart() {
		TestCli cli = new TestCli();
		tester.run(cli, "help", YarnClusterStartCommand.DEFAULT_COMMAND);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString(YarnClusterStartCommand.DEFAULT_COMMAND + " - " + YarnClusterStartCommand.DEFAULT_DESC));
	}

	@Test
	public void testHelpClusterstop() {
		TestCli cli = new TestCli();
		tester.run(cli, "help", YarnClusterStopCommand.DEFAULT_COMMAND);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString(YarnClusterStopCommand.DEFAULT_COMMAND + " - " + YarnClusterStopCommand.DEFAULT_DESC));
	}

	@Test
	public void testHelpClustermodify() {
		TestCli cli = new TestCli();
		tester.run(cli, "help", YarnClusterModifyCommand.DEFAULT_COMMAND);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString(YarnClusterModifyCommand.DEFAULT_COMMAND + " - " + YarnClusterModifyCommand.DEFAULT_DESC));
	}

	@Test
	public void testHelpClusterdestroy() {
		TestCli cli = new TestCli();
		tester.run(cli, "help", YarnClusterDestroyCommand.DEFAULT_COMMAND);
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString(YarnClusterDestroyCommand.DEFAULT_COMMAND + " - " + YarnClusterDestroyCommand.DEFAULT_DESC));
	}

	@Test
	public void testHelpCustomNativeCommand() {
		TestCliCustomNativeCommands cli = new TestCliCustomNativeCommands();
		tester.run(cli, "help", "custom");
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString("custom - Custom Native Command"));
	}

	@Test
	public void testRunCustomNativeCommand() {
		TestCliCustomNativeCommands cli = new TestCliCustomNativeCommands();
		tester.run(cli, "custom");
		String output = tester.getOutput();
		assertThat(cli.exitCode, is(0));
		assertThat(output, containsString("run custom command"));
	}

	private static class TestCli extends AbstractCli {

		Integer exitCode;

		public TestCli() {
			List<Command> commands = new ArrayList<Command>();
			commands.add(new YarnPushCommand());
			commands.add(new YarnPushedCommand());
			commands.add(new YarnSubmitCommand());
			commands.add(new YarnSubmittedCommand());
			commands.add(new YarnKillCommand());
			commands.add(new YarnClustersInfoCommand());
			commands.add(new YarnClusterInfoCommand());
			commands.add(new YarnClusterCreateCommand());
			commands.add(new YarnClusterStartCommand());
			commands.add(new YarnClusterStopCommand());
			commands.add(new YarnClusterModifyCommand());
			commands.add(new YarnClusterDestroyCommand());
			registerCommands(commands);
			registerCommand(new ShellCommand(commands));
		}

		@Override
		protected void handleRunnerExitCode(CommandRunner runner, int exitCode) {
			this.exitCode = exitCode;
		}

	}

	private static class TestCliCustomCommands extends AbstractCli {

		Integer exitCode;

		public TestCliCustomCommands() {
			List<Command> commands = new ArrayList<Command>();
			commands.add(new YarnPushCommand(YarnPushCommand.DEFAULT_COMMAND + "x", YarnPushCommand.DEFAULT_DESC + "x",
					new PushOptionHandler()));
			commands.add(new YarnPushedCommand(YarnPushedCommand.DEFAULT_COMMAND + "x", YarnPushedCommand.DEFAULT_DESC
					+ "x", new PushedOptionHandler()));
			commands.add(new YarnSubmitCommand(YarnSubmitCommand.DEFAULT_COMMAND + "x", YarnSubmitCommand.DEFAULT_DESC
					+ "x", new SubmitOptionHandler()));
			commands.add(new YarnSubmittedCommand(YarnSubmittedCommand.DEFAULT_COMMAND + "x",
					YarnSubmittedCommand.DEFAULT_DESC + "x", new SubmittedOptionHandler()));
			commands.add(new YarnKillCommand(YarnKillCommand.DEFAULT_COMMAND + "x", YarnKillCommand.DEFAULT_DESC + "x",
					new KillOptionHandler()));
			commands.add(new YarnClustersInfoCommand(YarnClustersInfoCommand.DEFAULT_COMMAND + "x",
					YarnClustersInfoCommand.DEFAULT_DESC + "x", new ClustersInfoOptionHandler()));
			commands.add(new YarnClusterInfoCommand(YarnClusterInfoCommand.DEFAULT_COMMAND + "x",
					YarnClusterInfoCommand.DEFAULT_DESC + "x", new ClusterInfoOptionHandler()));
			commands.add(new YarnClusterCreateCommand(YarnClusterCreateCommand.DEFAULT_COMMAND + "x",
					YarnClusterCreateCommand.DEFAULT_DESC + "x", new ClusterCreateOptionHandler()));
			commands.add(new YarnClusterStartCommand(YarnClusterStartCommand.DEFAULT_COMMAND + "x",
					YarnClusterStartCommand.DEFAULT_DESC + "x", new ClusterStartOptionHandler()));
			commands.add(new YarnClusterStopCommand(YarnClusterStopCommand.DEFAULT_COMMAND + "x",
					YarnClusterStopCommand.DEFAULT_DESC + "x", new ClusterStopOptionHandler()));
			commands.add(new YarnClusterModifyCommand(YarnClusterModifyCommand.DEFAULT_COMMAND + "x",
					YarnClusterModifyCommand.DEFAULT_DESC + "x", new ClusterModifyOptionHandler()));
			commands.add(new YarnClusterDestroyCommand(YarnClusterDestroyCommand.DEFAULT_COMMAND + "x",
					YarnClusterDestroyCommand.DEFAULT_DESC + "x", new ClusterDestroyOptionHandler()));
			registerCommands(commands);
			registerCommand(new ShellCommand(commands));
		}

		@Override
		protected void handleRunnerExitCode(CommandRunner runner, int exitCode) {
			this.exitCode = exitCode;
		}

	}

	private static class TestCliCustomNativeCommands extends AbstractCli {

		Integer exitCode;

		public TestCliCustomNativeCommands() {
			List<Command> commands = new ArrayList<Command>();
			commands.add(new CustomNativeCommand());
			registerCommands(commands);
		}

		@Override
		protected void handleRunnerExitCode(CommandRunner runner, int exitCode) {
			this.exitCode = exitCode;
		}

	}

	private static class CustomNativeCommand implements Command {

		@Override
		public String getName() {
			return "custom";
		}

		@Override
		public String getDescription() {
			return "Custom Native Command";
		}

		@Override
		public String getUsageHelp() {
			return "getUsageHelp";
		}

		@Override
		public String getHelp() {
			return "getHelp";
		}

		@Override
		public Collection<OptionHelp> getOptionsHelp() {
			OptionHelp optionHelp = new OptionHelp() {

				@Override
				public String getUsageHelp() {
					return null;
				}

				@Override
				public Set<String> getOptions() {
					return null;
				}
			};
			return Arrays.asList(optionHelp);
		}

		@Override
		public ExitStatus run(String... args) throws Exception {
			Log.info("run custom command");
			return ExitStatus.OK;
		}

		@Override
		public Collection<HelpExample> getExamples() {
			return null;
		}

	}

}
