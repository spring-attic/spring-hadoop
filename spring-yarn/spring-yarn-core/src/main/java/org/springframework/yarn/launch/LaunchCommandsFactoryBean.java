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

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * Factory bean helping to construct a command meant to
 * start application master or a container.
 *
 * @author Janne Valkealahti
 *
 */
public class LaunchCommandsFactoryBean implements InitializingBean, FactoryBean<String[]>{

	/** Main command */
	private String command = "java";

	/** Class to run */
	private Class<? extends AbstractCommandLineRunner<?>> runner;

	/** Spring context file argument */
	private String contextFile;

	/** Bean name in command line */
	private String beanName;

	/** Possible arguments */
	private Properties arguments;

	/** Stdout */
	private String stdout = "<LOG_DIR>/stdout";

	/** Stderr */
	private String stderr = "<LOG_DIR>/stderr";

	/** Commands returned from this factory */
	private String[] commands;

	@Override
	public String[] getObject() throws Exception {
		if(commands == null) {
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
		Assert.notNull(runner, "Main class must be set");
		Assert.notNull(contextFile, "Context file path must be set");
		Assert.notNull(beanName, "Bean name must be set");
		Assert.notNull(stdout, "Stdout must be set");
		Assert.notNull(stderr, "Stderr name must be set");

		List<String> commandsList = new ArrayList<String>();
		commandsList.add(command);

		// -D arguments needs to be right after main command
		if(arguments != null) {
			Enumeration<?> names = arguments.propertyNames();
			while (names.hasMoreElements()) {
				String key = (String) names.nextElement();
				if (key.startsWith("-D")) {
					commandsList.add(key + "=" + arguments.getProperty(key));					
				}
			}
		}
		
		commandsList.add(runner.getCanonicalName());
		commandsList.add(contextFile);
		commandsList.add(beanName);
		
		// arguments without -D
		if(arguments != null) {
			Enumeration<?> names = arguments.propertyNames();
			while (names.hasMoreElements()) {
				String key = (String) names.nextElement();
				if (!key.startsWith("-D")) {
					commandsList.add(key + "=" + arguments.getProperty(key));					
				}
			}
		}
		
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
	 * @param runner the new runner
	 */
	public void setRunner(Class<? extends AbstractCommandLineRunner<?>> runner) {
		this.runner = runner;
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

}
