/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.launch;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * Factory bean helping to construct a command meant to start application master
 * or a container.
 *
 * @author Janne Valkealahti
 *
 */
public class LaunchCommandsFactoryBean implements InitializingBean, FactoryBean<String[]> {

	/** Main command, default to <JAVA_HOME>/bin/java */
	private String command = ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java";

	/** File name indicating executable jar mode */
	private String jarFile;

	/** Class to run */
	private Class<?> runner;

	/** Class to run */
	private String runnerClass;

	/** Spring context file argument */
	private String contextFile;

	/** Bean name in command line */
	private String beanName;

	/** Possible arguments */
	private Properties arguments;

	/** Possible arguments as list */
	private List<String> argumentsList;

	/** Possible jvm options */
	private List<String> options;

	/** Stdout */
	private String stdout = "<LOG_DIR>/stdout";

	/** Stderr */
	private String stderr = "<LOG_DIR>/stderr";

	/** Commands returned from this factory */
	private String[] commands;

	@Override
	public String[] getObject() throws Exception {
		if (commands == null) {
			afterPropertiesSet();
		}
		return commands;
	}

	@Override
	public Class<String[]> getObjectType() {
		return String[].class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(command, "Main command must be set");
		Assert.notNull(stdout, "Stdout must be set");
		Assert.notNull(stderr, "Stderr name must be set");

		List<String> commandsList = new ArrayList<String>();
		commandsList.add(command);

		// jvm options right after main command
		commandsList.addAll(resolveJvmOptions());

		if (jarFile != null) {
			commandsList.add("-jar");
			commandsList.add(jarFile);
		} else {
			if (runnerClass != null) {
				commandsList.add(runnerClass);
			} else if (runner != null) {
				commandsList.add(runner.getCanonicalName());
			}
			if (contextFile != null) {
				commandsList.add(contextFile);
			}
			if (beanName != null) {
				commandsList.add(beanName);
			}
		}

		if (argumentsList != null) {
			commandsList.addAll(argumentsList);
		}

		// program arguments at the end before stdout/stderr
		commandsList.addAll(resolveProgramArguments());

		commandsList.add("1>" + stdout);
		commandsList.add("2>" + stderr);
		commands = commandsList.toArray(new String[0]);
	}

	/**
	 * Sets the main command.
	 *
	 * @param command the new command
	 */
	public void setCommand(String command) {
		this.command = command;
	}

	/**
	 * Sets the runner class.
	 *
	 * @param runner the new runner class
	 */
	public void setRunner(Class<?> runner) {
		this.runner = runner;
	}

	/**
	 * Sets the runner class.
	 *
	 * @param runnerClass the new runner class
	 */
	public void setRunnerClass(String runnerClass) {
		this.runnerClass = runnerClass;
	}

	/**
	 * Sets the jar file name. If this is set, the command mode is automatically
	 * to use executable jar with 'java -jar'.
	 *
	 * @param jarFile the new jar file
	 */
	public void setJarFile(String jarFile) {
		this.jarFile = jarFile;
	}

	/**
	 * Sets the context file.
	 *
	 * @param contextFile the new context file
	 */
	public void setContextFile(String contextFile) {
		this.contextFile = contextFile;
	}

	/**
	 * Sets the bean name.
	 *
	 * @param beanName the new bean name
	 */
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	/**
	 * Sets the arguments.
	 *
	 * @param arguments the new arguments
	 */
	public void setArguments(Properties arguments) {
		this.arguments = arguments;
	}

	/**
	 * Sets the arguments as a list. Arguments set using
	 * this method are handled before arguments set via
	 * {@link #setArguments(Properties)}.
	 *
	 * @param argumentsList the new arguments
	 */
	public void setArgumentsList(List<String> argumentsList) {
		this.argumentsList = argumentsList;
	}

	/**
	 * Sets the options.
	 *
	 * @param options the new options
	 */
	public void setOptions(List<String> options) {
		this.options = options;
	}

	/**
	 * Sets the stdout.
	 *
	 * @param stdout the new stdout
	 */
	public void setStdout(String stdout) {
		this.stdout = stdout;
	}

	/**
	 * Sets the stderr.
	 *
	 * @param stderr the new stderr
	 */
	public void setStderr(String stderr) {
		this.stderr = stderr;
	}

	private List<String> resolveJvmOptions() {
		ArrayList<String> list = new ArrayList<String>();

		if (options != null) {
			list.addAll(options);
		}

		if (arguments != null) {
			// we still have legacy support for getting
			// -D's from arguments
			Enumeration<?> names = arguments.propertyNames();
			while (names.hasMoreElements()) {
				String key = (String) names.nextElement();
				if (key.startsWith("-D")) {
					String opt = key + "=" + arguments.getProperty(key);
					if (options != null) {
						if (!containsStartsWith(options, key + "=")) {
							list.add(opt);
						}
					} else {
						list.add(opt);
					}
				}
			}
		}

		return list;
	}

	private List<String> resolveProgramArguments() {
		ArrayList<String> list = new ArrayList<String>();

		if (arguments != null) {
			Enumeration<?> names = arguments.propertyNames();
			while (names.hasMoreElements()) {
				String key = (String) names.nextElement();
				if (!key.startsWith("-D")) {
					list.add(key + "=" + arguments.getProperty(key));
				}
			}
		}

		return list;
	}

	private static boolean containsStartsWith(List<String> options, String key) {
		for (String option : options) {
			if (option.startsWith(key)) {
				return true;
			}
		}
		return false;
	}

}
