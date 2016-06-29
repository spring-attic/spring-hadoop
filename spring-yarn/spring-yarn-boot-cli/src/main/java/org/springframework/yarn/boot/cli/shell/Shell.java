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
package org.springframework.yarn.boot.cli.shell;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jline.console.ConsoleReader;
import jline.console.completer.CandidateListCompletionHandler;

import org.fusesource.jansi.AnsiRenderer.Code;
import org.springframework.boot.cli.command.Command;
import org.springframework.boot.cli.command.CommandRunner;
import org.springframework.boot.cli.command.core.HelpCommand;
import org.springframework.boot.loader.tools.SignalUtils;
import org.springframework.util.StringUtils;

/**
 * A shell for Spring YARN Cli. Drops the user into an event loop (REPL) where command line
 * completion and history are available without relying on OS shell features.
 *
 * @author Jon Brisbin
 * @author Dave Syer
 * @author Phillip Webb
 * @author Janne Valkealahti
 */
public class Shell {

	private final ShellCommandRunner commandRunner;

	private final ConsoleReader consoleReader;

	private final EscapeAwareWhiteSpaceArgumentDelimiter argumentDelimiter = new EscapeAwareWhiteSpaceArgumentDelimiter();

	private final ShellPrompts prompts = new ShellPrompts();

	/**
	 * Create a new {@link Shell} instance.
	 * @param commands Commands
	 * @throws IOException on error
	 */
	public Shell(List<Command> commands) throws IOException {
		attachSignalHandler();
		this.consoleReader = new ConsoleReader();
		this.commandRunner = createCommandRunner(commands);
		initializeConsoleReader();
	}

	private ShellCommandRunner createCommandRunner(List<Command> commands) {
		ShellCommandRunner runner = new ShellCommandRunner();
		runner.addCommand(new HelpCommand(runner));
		if (commands != null) {
			runner.addCommands(commands);
		}
		runner.addCommand(new PromptCommand(this.prompts));
		runner.addCommand(new ClearCommand(this.consoleReader));
		runner.addCommand(new ExitCommand());
		runner.addAliases("exit", "quit");
		runner.addAliases("help", "?");
		runner.addAliases("clear", "cls");
		return runner;
	}

	private void initializeConsoleReader() {
		this.consoleReader.setHistoryEnabled(true);
		this.consoleReader.setBellEnabled(false);
		this.consoleReader.setExpandEvents(false);
		this.consoleReader.addCompleter(new CommandCompleter(this.consoleReader,
				this.argumentDelimiter, this.commandRunner));
		this.consoleReader.setCompletionHandler(new CandidateListCompletionHandler());
	}

	private void attachSignalHandler() {
		SignalUtils.attachSignalHandler(new Runnable() {
			@Override
			public void run() {
				handleSigInt();
			}
		});
	}

	/**
	 * Run the shell until the user exists.
	 * @throws Exception on error
	 */
	public void run() throws Exception {
		printBanner();
		try {
			runInputLoop();
		}
		catch (Exception ex) {
			if (!(ex instanceof ShellExitException)) {
				throw ex;
			}
		}
	}

	private void printBanner() {
		String version = getClass().getPackage().getImplementationVersion();
		version = (version == null ? "" : " (v" + version + ")");
		System.out.println(ansi("Spring YARN Cli", Code.BOLD).append(version, Code.FAINT));
		System.out.println(ansi("Hit TAB to complete. Type 'help' and hit "
				+ "RETURN for help, and 'exit' to quit."));
	}

	private void runInputLoop() throws Exception {
		String line;
		while ((line = this.consoleReader.readLine(getPrompt())) != null) {
			while (line.endsWith("\\")) {
				line = line.substring(0, line.length() - 1);
				line += this.consoleReader.readLine("> ");
			}
			if (StringUtils.hasLength(line)) {
				String[] args = this.argumentDelimiter.parseArguments(line);
				this.commandRunner.runAndHandleErrors(args);
			}
		}
	}

	private String getPrompt() {
		String prompt = this.prompts.getPrompt();
		return ansi(prompt, Code.FG_BLUE).toString();
	}

	private AnsiString ansi(String text, Code... codes) {
		return new AnsiString(this.consoleReader.getTerminal()).append(text, codes);
	}

	/**
	 * Final handle an interrup signal (CTRL-C)
	 */
	protected void handleSigInt() {
		if (this.commandRunner.handleSigInt()) {
			return;
		}
		System.out.println("\nThanks for using Spring Boot");
		System.exit(1);
	}

	/**
	 * Extension of {@link CommandRunner} to deal with {@link RunProcessCommand}s and
	 * aliases.
	 */
	private class ShellCommandRunner extends CommandRunner {

		private volatile Command lastCommand;

		private final Map<String, String> aliases = new HashMap<String, String>();

		public ShellCommandRunner() {
			super(null);
		}

		public void addAliases(String command, String... aliases) {
			for (String alias : aliases) {
				this.aliases.put(alias, command);
			}
		}

		@Override
		public Command findCommand(String name) {
			if (name.startsWith("!")) {
				return new RunProcessCommand(name.substring(1));
			}
			if (this.aliases.containsKey(name)) {
				name = this.aliases.get(name);
			}
			return super.findCommand(name);
		}

		@Override
		protected void beforeRun(Command command) {
			this.lastCommand = command;
		}

		@Override
		protected void afterRun(Command command) {
		}

		public boolean handleSigInt() {
			Command command = this.lastCommand;
			if (command != null && command instanceof RunProcessCommand) {
				return ((RunProcessCommand) command).handleSigInt();
			}
			return false;
		}

	}

}
