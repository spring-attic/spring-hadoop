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
package org.springframework.yarn.container;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.annotation.YarnEnvironment;
import org.springframework.yarn.annotation.YarnEnvironments;
import org.springframework.yarn.annotation.YarnParameter;
import org.springframework.yarn.annotation.YarnParameters;

/**
 * Tests for {@link MethodInvokingYarnContainerRuntimeProcessor}.
 *
 * @author Janne Valkealahti
 *
 */
public class MethodInvokingYarnContainerRuntimeProcessorTests {

	@Test
	public void testVoidMethodNameCalled() throws Exception {
		TestBean3 bean = new TestBean3();
		MethodInvokingYarnContainerRuntimeProcessor<Object> processor =
				new MethodInvokingYarnContainerRuntimeProcessor<Object>(bean, "test");
		Object process = processor.process(new TestYarnContainerRuntime());
		assertNull(process);
		assertTrue(bean.testCalled);
	}

	@Test
	public void testIntMethodNameCalled() throws Exception {
		MethodInvokingYarnContainerRuntimeProcessor<Object> processor =
				new MethodInvokingYarnContainerRuntimeProcessor<Object>(new TestBean1(), "test");
		Object process = processor.process(new TestYarnContainerRuntime());
		assertNotNull(process);
		assertThat(process, instanceOf(Integer.class));
		assertThat((Integer)process, is(1));
	}

	@Test
	public void testIntAllAnnotations() throws Exception {
		TestBean2 bean = new TestBean2();
		MethodInvokingYarnContainerRuntimeProcessor<Object> processor =
				new MethodInvokingYarnContainerRuntimeProcessor<Object>(bean, "test");
		Object process = processor.process(new TestYarnContainerRuntime(2));
		assertNotNull(process);
		assertThat(process, instanceOf(Integer.class));
		assertThat((Integer)process, is(4));
		assertThat((Integer)process, is(4));
		assertThat(bean.environment.size(), is(2));
		assertThat(bean.parameters.size(), is(2));
	}

	@Test
	public void testVoidParameter() throws Exception {
		TestBean4 bean = new TestBean4();
		MethodInvokingYarnContainerRuntimeProcessor<Object> processor =
				new MethodInvokingYarnContainerRuntimeProcessor<Object>(bean, "test");
		Object process = processor.process(new TestYarnContainerRuntime(2));
		assertNull(process);
		assertThat(bean.parameter, is("param-value1"));
		assertThat(bean.env, is("env-value1"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testShoulFailWithTwoOnYarnContainerStartAnnotations() throws Exception {
		new MethodInvokingYarnContainerRuntimeProcessor<Object>(new TestBean5(), OnContainerStart.class);
	}

	@Test(expected = IllegalStateException.class)
	public void testNoMatch() throws Exception {
		new MethodInvokingYarnContainerRuntimeProcessor<Object>(new TestBean3(), "nomatch");
	}

	@Test
	public void testVoidWithOnYarnContainerStartAnnotations() throws Exception {
		TestBean6 bean = new TestBean6();
		MethodInvokingYarnContainerRuntimeProcessor<Object> processor =
				new MethodInvokingYarnContainerRuntimeProcessor<Object>(bean, OnContainerStart.class);
		Object process = processor.process(new TestYarnContainerRuntime());
		assertNull(process);
		assertTrue(bean.testCalled);
	}

	private static class TestBean1 {
		@SuppressWarnings("unused")
		public int test() {
			return 1;
		}
	}

	private static class TestBean2 {
		Map<String, String> environment;
		Properties parameters;
		@SuppressWarnings("unused")
		public int test(@YarnEnvironments Map<String, String> environment, @YarnParameters Properties parameters) {
			this.environment = environment;
			this.parameters = parameters;
			return (environment != null ? environment.size() : 0) + (parameters != null ? parameters.size() : 0);
		}
	}

	private static class TestBean3 {
		boolean testCalled;
		@SuppressWarnings("unused")
		public void test() {
			testCalled = true;
		}
	}

	private static class TestBean4 {
		String parameter;
		String env;
		@SuppressWarnings("unused")
		public void test(@YarnParameter("param-key1") String parameter, @YarnEnvironment("env-key1") String env) {
			this.parameter = parameter;
			this.env = env;
		}
	}

	private static class TestBean5 {
		@OnContainerStart
		public void test1() {
		}
		@OnContainerStart
		public void test2() {
		}
	}

	private static class TestBean6 {
		boolean testCalled;
		@OnContainerStart
		public void test() {
			testCalled = true;
		}
	}

	private static class TestYarnContainerRuntime implements YarnContainerRuntime {
		private Map<String, String> environment;
		private Properties parameters;
		public TestYarnContainerRuntime() {
			this(0);
		}
		public TestYarnContainerRuntime(int count) {
			environment = new HashMap<String, String>();
			parameters = new Properties();
			for (int i = 0; i < count; i++) {
				String key = "key" + i;
				String value = "value" + i;
				environment.put("env-" + key, "env-" + value);
				parameters.put("param-" + key, "param-" + value);
			}
		}
		@Override
		public Map<String, String> getEnvironment() {
			return environment;
		}
		@Override
		public Properties getParameters() {
			return parameters;
		}
	}

}
