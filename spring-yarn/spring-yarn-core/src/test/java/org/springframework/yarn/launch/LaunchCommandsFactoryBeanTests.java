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
package org.springframework.yarn.launch;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

/**
 * Tests for {@link LaunchCommandsFactoryBean}.
 *
 * @author Janne Valkealahti
 *
 */
public class LaunchCommandsFactoryBeanTests {

	@Test
	public void testDefault() throws Exception {
		LaunchCommandsFactoryBean factory = new LaunchCommandsFactoryBean();
		factory.afterPropertiesSet();
		String[] commands = factory.getObject();
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(3));
		assertThat(commands[0], is("$JAVA_HOME/bin/java"));
		assertThat(commands[1], is("1><LOG_DIR>/stdout"));
		assertThat(commands[2], is("2><LOG_DIR>/stderr"));
	}

	@Test
	public void testStdoutStderr() throws Exception {
		LaunchCommandsFactoryBean factory = new LaunchCommandsFactoryBean();
		factory.setStdout("out1");
		factory.setStderr("err1");
		factory.afterPropertiesSet();
		String[] commands = factory.getObject();
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(3));
		assertThat(commands[0], is("$JAVA_HOME/bin/java"));
		assertThat(commands[1], is("1>out1"));
		assertThat(commands[2], is("2>err1"));
	}

	@Test
	public void testCommand() throws Exception {
		String[] commands = createCommands("fakecommand", null, null, null, null, null, null, null, null, null);
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(3));
		assertThat(commands[0], is("fakecommand"));
	}

	@Test
	public void testJar() throws Exception {
		Properties args = createProperties("foo1", "jee1", "-Dfoo2", "jee2");
		List<String> opts = createOptions("-Xms512m");
		String[] commands = createCommands(null, null, null, "foo.jar", null, null, opts, args, null, null);
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(8));
		assertThat(commands[1], is("-Xms512m"));
		assertThat(commands[2], is("-Dfoo2=jee2"));
		assertThat(commands[3], is("-jar"));
		assertThat(commands[4], is("foo.jar"));
		assertThat(commands[5], is("foo1=jee1"));
	}

	@Test
	public void testArguments() throws Exception {
		Properties p = createProperties("foo1", "jee1");
		String[] commands = createCommands(null, null, null, null, null, null, null, p, null, null);
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(4));
		assertThat(commands[1], is("foo1=jee1"));

		commands = createCommands(null, null, null, "foo.jar", null, null, null, p, null, null);
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(6));
		assertThat(commands[1], is("-jar"));
		assertThat(commands[2], is("foo.jar"));
		assertThat(commands[3], is("foo1=jee1"));

		p = createProperties("-Dfoo1", "jee1");
		commands = createCommands(null, null, null, null, null, null, null, p, null, null);
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(4));
		assertThat(commands[1], is("-Dfoo1=jee1"));

		commands = createCommands(null, null, null, "foo.jar", null, null, null, p, null, null);
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(6));
		assertThat(commands[1], is("-Dfoo1=jee1"));
		assertThat(commands[2], is("-jar"));
		assertThat(commands[3], is("foo.jar"));
	}

	@Test
	public void testArgumentsAndOptions() throws Exception {
		Properties args = createProperties("foo1", "jee1");
		List<String> opts = createOptions("-Xms512m");
		String[] commands = createCommands(null, null, null, null, null, null, opts, args, null, null);
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(5));
		assertThat(commands[1], is("-Xms512m"));
		assertThat(commands[2], is("foo1=jee1"));

		// check that same is not added twice
		args = createProperties("-Dfoo", "jee2");
		opts = createOptions("-Xfoojee2", "-Dfoo=jee2");
		commands = createCommands(null, null, null, null, null, null, opts, args, null, null);
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(5));
		assertThat(commands[1], is("-Xfoojee2"));
		assertThat(commands[2], is("-Dfoo=jee2"));

		// check that opt is not overwritten
		args = createProperties("-Dfoo", "jee1");
		opts = createOptions("-Dfoo=jee2");
		commands = createCommands(null, null, null, null, null, null, opts, args, null, null);
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(4));
		assertThat(commands[1], is("-Dfoo=jee2"));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testNullCommand() throws Exception {
		LaunchCommandsFactoryBean factory = new LaunchCommandsFactoryBean();
		factory.setCommand(null);
		factory.afterPropertiesSet();
		factory.getObject();
	}

	@Test(expected=IllegalArgumentException.class)
	public void testNullStdout() throws Exception {
		LaunchCommandsFactoryBean factory = new LaunchCommandsFactoryBean();
		factory.setStdout(null);
		factory.afterPropertiesSet();
		factory.getObject();
	}

	@Test(expected=IllegalArgumentException.class)
	public void testNullStderr() throws Exception {
		LaunchCommandsFactoryBean factory = new LaunchCommandsFactoryBean();
		factory.setStderr(null);
		factory.afterPropertiesSet();
		factory.getObject();
	}

	@Test
	public void testRunner() throws Exception {
		Properties p = createProperties("foo1", "jee1", "-Dfoo2", "jee2");

		String[] commands = createCommands(null, null, "foo.jee.RunnerClazz", null, null, null, null, null, null, null);
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(4));
		assertThat(commands[1], is("foo.jee.RunnerClazz"));

		commands = createCommands(null, null, "foo.jee.RunnerClazz", null, null, null, null, p, null, null);
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(6));
		assertThat(commands[1], is("-Dfoo2=jee2"));
		assertThat(commands[2], is("foo.jee.RunnerClazz"));
		assertThat(commands[3], is("foo1=jee1"));

		commands = createCommands(null, null, "foo.jee.RunnerClazz", null, "contextFile", "beanName", null, null, null, null);
		assertThat(commands, notNullValue());
		assertThat(commands.length, is(6));
		assertThat(commands[1], is("foo.jee.RunnerClazz"));
		assertThat(commands[2], is("contextFile"));
		assertThat(commands[3], is("beanName"));
	}

	private static List<String> createOptions(String... opts) {
		ArrayList<String> list = new ArrayList<String>();
		for (String opt : opts) {
			list.add(opt);
		}
		return list;
	}

	private static Properties createProperties(String... props) {
		Properties p = new Properties();
		for (int i = 0; i<props.length; i+=2) {
			p.put(props[i], props[i+1]);
		}
		return p;
	}

	private static String[] createCommands(String command, Class<?> runner, String runnerClass, String jarFile,
			String contextFile, String beanName, List<String> options, Properties arguments, String stdout, String stderr) throws Exception {
		LaunchCommandsFactoryBean factory = new LaunchCommandsFactoryBean();
		if (command != null) {
			factory.setCommand(command);
		}
		if (runner != null) {
			factory.setRunner(runner);
		}
		if (runnerClass != null) {
			factory.setRunnerClass(runnerClass);
		}
		if (jarFile != null) {
			factory.setJarFile(jarFile);
		}
		if (contextFile != null) {
			factory.setContextFile(contextFile);
		}
		if (beanName != null) {
			factory.setBeanName(beanName);
		}
		if (options != null) {
			factory.setOptions(options);
		}
		if (arguments != null) {
			factory.setArguments(arguments);
		}
		if (stdout != null) {
			factory.setStdout(stdout);
		}
		if (stderr != null) {
			factory.setStderr(stderr);
		}
		factory.afterPropertiesSet();
		return factory.getObject();
	}

}
