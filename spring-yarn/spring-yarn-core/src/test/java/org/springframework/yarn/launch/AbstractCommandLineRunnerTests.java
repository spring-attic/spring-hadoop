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

import java.util.List;
import java.util.Set;

import org.junit.After;
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

	private String ctxConfigPath = ClassUtils.addResourcePathToPackagePath(
			AbstractCommandLineRunnerTests.class, "AbstractCommandLineRunnerTests-run.xml");

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {

	}

	@Test
	public void testWithArgs() {
		String[] args = new String[] {ctxConfigPath, "stubTestBean1"};
		StubCommandLineRunner.presetSystemExiter(new StubSystemExiter());
		StubCommandLineRunner.main(args);
		assertEquals(0, StubSystemExiter.status);
		String errorMessage = StubCommandLineRunner.getErrorMessage();
		assertThat(0, is(errorMessage.length()));
	}

	@Test
	public void testInvalidArgs() throws Exception {
		String[] args = new String[] {};
		StubCommandLineRunner.presetSystemExiter(new StubSystemExiter());
		StubCommandLineRunner.main(args);
		assertEquals(1, StubSystemExiter.status);
		String errorMessage = StubCommandLineRunner.getErrorMessage();
		assertTrue("Wrong error message: " + errorMessage, errorMessage.contains("Config locations must not be null"));
	}

	public static class StubCommandLineRunner extends AbstractCommandLineRunner<StubBean> {

		@Override
		protected String getDefaultBeanIdentifier() {
			return "stubTestBean";
		}

		@Override
		protected List<String> getValidOpts() {
			return null;
		}

		public static void main(String[] args) {
			StubCommandLineRunner runner = new StubCommandLineRunner();
			runner.doMain(args);
		}

		@Override
		protected void handleBeanRun(StubBean bean, String[] parameters, Set<String> opts) {
			if(!"data".equals(bean.getData())) {
				throw new RuntimeException();
			}
		}

	}

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
