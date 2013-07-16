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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.springframework.util.ClassUtils;

/**
 * Tests for {@link AbstractCommandLineRunner} base functionality.
 *
 * @author Janne Valkealahti
 *
 */
public class AbstractCommandLineRunnerTests {

	private final static String ctxConfigPath = ClassUtils.addResourcePathToPackagePath(
			AbstractCommandLineRunnerTests.class, "AbstractCommandLineRunnerTests-run.xml");

	private final static String childCtxConfigPath = ClassUtils.addResourcePathToPackagePath(
			AbstractCommandLineRunnerTests.class, "AbstractCommandLineRunnerTests-child.xml");

	@Before
	public void setup() {
		StubCommandLineRunner.presetSystemExiter(new StubSystemExiter());
		StubCommandLineRunner.opts = null;
		StubCommandLineRunner.parameters = null;
	}

	@Test
	public void testWithArgs() {
		String[] args = new String[] {ctxConfigPath, "stubTestBean"};
		StubCommandLineRunner.main(args);
		assertEquals(0, StubSystemExiter.status);
		String error = StubCommandLineRunner.getErrorMessage();
		assertThat(0, is(error.length()));
	}

	@Test
	public void testWithArgsWithParams() {
		String[] args = new String[] {ctxConfigPath, "stubTestBean", "foo1=jee1"};
		StubCommandLineRunner.main(args);
		assertEquals(0, StubSystemExiter.status);
		assertEquals(1, StubCommandLineRunner.parameters.length);
		assertEquals("foo1=jee1", StubCommandLineRunner.parameters[0]);
		String error = StubCommandLineRunner.getErrorMessage();
		assertThat(0, is(error.length()));
	}

	@Test
	public void testMissingArgs() throws Exception {
		String[] args = new String[] {};
		StubCommandLineRunner.main(args);
		assertEquals(1, StubSystemExiter.status);
		String error = StubCommandLineRunner.getErrorMessage();
		assertTrue("Wrong error message: " + error, error.contains("Config locations must not be null"));
	}

	@Test
	public void testMissingBeanName() throws Exception {
		String[] args = new String[] {ctxConfigPath};
		StubCommandLineRunner.main(args);
		assertEquals(0, StubSystemExiter.status);
		String error = StubCommandLineRunner.getErrorMessage();
		assertThat(0, is(error.length()));
	}

	@Test
	public void testMissingBeanNameWithParams() throws Exception {
		String[] args = new String[] {ctxConfigPath, "foo1=jee1"};
		StubCommandLineRunner.main(args);
		assertEquals(0, StubSystemExiter.status);
		assertEquals(1, StubCommandLineRunner.parameters.length);
		assertEquals("foo1=jee1", StubCommandLineRunner.parameters[0]);
		String error = StubCommandLineRunner.getErrorMessage();
		assertThat(0, is(error.length()));
	}

	@Test
	public void testWithArgsAndOptions1() {
		String[] args = new String[] {"-option1", ctxConfigPath, "stubTestBean"};
		StubCommandLineRunner.opts = new String[]{"-option1", "-option2"};
		StubCommandLineRunner.main(args);
		assertEquals(0, StubSystemExiter.status);
		String error = StubCommandLineRunner.getErrorMessage();
		assertThat(0, is(error.length()));
	}

	@Test
	public void testWithArgsAndOptions2() {
		String[] args = new String[] {ctxConfigPath, "-option1", "stubTestBean"};
		StubCommandLineRunner.opts = new String[]{"-option1", "-option2"};
		StubCommandLineRunner.main(args);
		assertEquals(0, StubSystemExiter.status);
		String error = StubCommandLineRunner.getErrorMessage();
		assertThat(0, is(error.length()));
	}

	@Test
	public void testWithArgsAndOptions3() {
		String[] args = new String[] {ctxConfigPath, "stubTestBean", "-option1"};
		StubCommandLineRunner.opts = new String[]{"-option1", "-option2"};
		StubCommandLineRunner.main(args);
		assertEquals(0, StubSystemExiter.status);
		String error = StubCommandLineRunner.getErrorMessage();
		assertThat(0, is(error.length()));
	}

	@Test
	public void testWithArgsAndOptions4() {
		String[] args = new String[] {"-option2", ctxConfigPath, "-option1", "stubTestBean"};
		StubCommandLineRunner.opts = new String[]{"-option1", "-option2"};
		StubCommandLineRunner.main(args);
		assertEquals(0, StubSystemExiter.status);
		String error = StubCommandLineRunner.getErrorMessage();
		assertThat(0, is(error.length()));
	}

	@Test
	public void testWithChildContext() {
		String[] args = new String[] {ctxConfigPath + "," + childCtxConfigPath, "stubTestBean"};
		StubCommandLineRunner.main(args);
		assertEquals(0, StubSystemExiter.status);
		String error = StubCommandLineRunner.getErrorMessage();
		assertThat(0, is(error.length()));
	}


	public static class StubCommandLineRunner extends AbstractCommandLineRunner<StubBean> {

		public static String[] opts = null;
		public static String[] parameters = null;

		@Override
		protected String getDefaultBeanIdentifier() {
			return "stubTestBean";
		}

		@Override
		protected List<String> getValidOpts() {
			if (opts != null) {
				return Arrays.asList(opts);
			} else {
				return null;
			}
		}

		public static void main(String[] args) {
			StubCommandLineRunner runner = new StubCommandLineRunner();
			runner.doMain(args);
		}

		@Override
		protected ExitStatus handleBeanRun(StubBean bean, String[] parameters, Set<String> opts) {
			StubCommandLineRunner.parameters = parameters;
			if(!"data".equals(bean.getData())) {
				throw new RuntimeException();
			}
			return ExitStatus.COMPLETED;
		}

	}

	/**
	 * Stub to know what exit value was used without
	 * doing System.exit().
	 */
	public static class StubSystemExiter implements SystemExiter {

		private static int status;

		@Override
		public void exit(int status) {
			StubSystemExiter.status = status;
		}

		public static int getStatus() {
			return status;
		}
	}

	/**
	 * Just stub bean used in test contexts.
	 */
	public static class StubBean {

		String data;

		public StubBean(){}

		public StubBean(String data){
			this.data = data;
		}

		public String getData() {
			return data;
		}

		public void setData(String data) {
			this.data = data;
		}

	}

}
