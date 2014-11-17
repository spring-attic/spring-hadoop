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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.annotation.YarnComponent;
import org.springframework.yarn.config.annotation.SpringYarnAnnotationPostProcessor;
import org.springframework.yarn.launch.ExitStatus;
import org.springframework.yarn.listener.ContainerStateListener;
import org.springframework.yarn.listener.ContainerStateListener.ContainerState;

/**
 * Tests for {@link DefaultYarnContainer}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultYarnContainerTests {

	@Test
	public void testContainerVoidBean() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, TestBean1Config.class);
		DefaultYarnContainer container = context.getBean(DefaultYarnContainer.class);
		final StateWrapper stateWrapper = new StateWrapper();

		container.addContainerStateListener(new ContainerStateListener() {
			@Override
			public void state(ContainerState state, Object exit) {
				stateWrapper.state = state;
				stateWrapper.exit = exit;
			}
		});

		container.run();
		assertThat(stateWrapper.state, is(ContainerState.COMPLETED));
		assertThat(stateWrapper.exit, instanceOf(Integer.class));
		assertThat((Integer)stateWrapper.exit, is(0));

		context.stop();
	}

	@Test
	public void testContainerBooleanBean() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, TestBean2Config.class);
		DefaultYarnContainer container = context.getBean(DefaultYarnContainer.class);
		final StateWrapper stateWrapper = new StateWrapper();

		container.addContainerStateListener(new ContainerStateListener() {
			@Override
			public void state(ContainerState state, Object exit) {
				stateWrapper.state = state;
				stateWrapper.exit = exit;
			}
		});

		container.run();
		assertThat(stateWrapper.state, is(ContainerState.COMPLETED));
		assertThat(stateWrapper.exit, instanceOf(Boolean.class));
		assertThat((Boolean)stateWrapper.exit, is(true));

		context.stop();
	}

	@Test
	public void testContainerIntRetBean() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, TestBean3Config.class);
		DefaultYarnContainer container = context.getBean(DefaultYarnContainer.class);
		final StateWrapper stateWrapper = new StateWrapper();

		container.addContainerStateListener(new ContainerStateListener() {
			@Override
			public void state(ContainerState state, Object exit) {
				stateWrapper.state = state;
				stateWrapper.exit = exit;
			}
		});

		container.run();
		assertThat(stateWrapper.state, is(ContainerState.COMPLETED));
		assertThat(stateWrapper.exit, instanceOf(Integer.class));
		assertThat((Integer)stateWrapper.exit, is(10));

		context.stop();
	}

	@Test
	public void testContainerBeanThrowsException() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, TestBean4Config.class);
		DefaultYarnContainer container = context.getBean(DefaultYarnContainer.class);
		final StateWrapper stateWrapper = new StateWrapper();

		container.addContainerStateListener(new ContainerStateListener() {
			@Override
			public void state(ContainerState state, Object exit) {
				stateWrapper.state = state;
				stateWrapper.exit = exit;
			}
		});

		container.run();
		assertThat(stateWrapper.state, is(ContainerState.FAILED));
		assertThat(stateWrapper.exit, instanceOf(Exception.class));

		context.stop();
	}

	@Test
	public void testContainerStringBean() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, TestBean6Config.class);
		DefaultYarnContainer container = context.getBean(DefaultYarnContainer.class);
		final StateWrapper stateWrapper = new StateWrapper();

		container.addContainerStateListener(new ContainerStateListener() {
			@Override
			public void state(ContainerState state, Object exit) {
				stateWrapper.state = state;
				stateWrapper.exit = exit;
			}
		});

		container.run();
		assertThat(stateWrapper.state, is(ContainerState.COMPLETED));
		assertThat(stateWrapper.exit, instanceOf(String.class));
		assertThat((String)stateWrapper.exit, is("UNKNOWN"));

		context.stop();
	}

	@Test
	public void testMultipleClasses() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, MultipleClassesConfig.class);
		DefaultYarnContainer container = context.getBean(DefaultYarnContainer.class);
		final StateWrapper stateWrapper = new StateWrapper();

		container.addContainerStateListener(new ContainerStateListener() {
			@Override
			public void state(ContainerState state, Object exit) {
				stateWrapper.state = state;
				stateWrapper.exit = exit;
			}
		});

		container.run();
		assertThat(stateWrapper.state, is(ContainerState.COMPLETED));
		assertThat(stateWrapper.exit, instanceOf(Integer.class));
		assertThat((Integer)stateWrapper.exit, is(0));

		TestBean1 testBean1 = context.getBean(TestBean1.class);
		TestBean7 testBean7 = context.getBean(TestBean7.class);
		assertThat(testBean1.called, is(true));
		assertThat(testBean7.called, is(true));

		context.stop();
	}

	@Test
	public void testMultipleMethods() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, MultipleMethodsConfig.class);
		DefaultYarnContainer container = context.getBean(DefaultYarnContainer.class);
		final StateWrapper stateWrapper = new StateWrapper();

		container.addContainerStateListener(new ContainerStateListener() {
			@Override
			public void state(ContainerState state, Object exit) {
				stateWrapper.state = state;
				stateWrapper.exit = exit;
			}
		});

		container.run();
		assertThat(stateWrapper.state, is(ContainerState.COMPLETED));
		assertThat(stateWrapper.exit, instanceOf(Integer.class));
		assertThat((Integer)stateWrapper.exit, is(0));

		TestBean5 testBean5 = context.getBean(TestBean5.class);
		assertThat(testBean5.called1, is(true));
		assertThat(testBean5.called2, is(true));

		context.stop();
	}

	@Test
	public void testMultipleClassesAndMethods() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, MultipleClassesAndMethodsConfig.class);
		DefaultYarnContainer container = context.getBean(DefaultYarnContainer.class);
		final StateWrapper stateWrapper = new StateWrapper();

		container.addContainerStateListener(new ContainerStateListener() {
			@Override
			public void state(ContainerState state, Object exit) {
				stateWrapper.state = state;
				stateWrapper.exit = exit;
			}
		});

		container.run();
		assertThat(stateWrapper.state, is(ContainerState.COMPLETED));
		assertThat(stateWrapper.exit, instanceOf(Integer.class));
		assertThat((Integer)stateWrapper.exit, is(0));

		TestBean1 testBean1 = context.getBean(TestBean1.class);
		TestBean7 testBean7 = context.getBean(TestBean7.class);
		TestBean5 testBean5 = context.getBean(TestBean5.class);
		assertThat(testBean1.called, is(true));
		assertThat(testBean7.called, is(true));
		assertThat(testBean5.called1, is(true));
		assertThat(testBean5.called2, is(true));

		context.stop();
	}

	@Test
	public void testOrderingByClass() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, OrderingByClassConfig.class);
		DefaultYarnContainer container = context.getBean(DefaultYarnContainer.class);
		final StateWrapper stateWrapper = new StateWrapper();

		container.addContainerStateListener(new ContainerStateListener() {
			@Override
			public void state(ContainerState state, Object exit) {
				stateWrapper.state = state;
				stateWrapper.exit = exit;
			}
		});

		container.run();
		assertThat(stateWrapper.state, is(ContainerState.COMPLETED));
		assertThat(stateWrapper.exit, instanceOf(Integer.class));
		assertThat((Integer)stateWrapper.exit, is(0));

		TestBean8 testBean8 = context.getBean(TestBean8.class);
		TestBean9 testBean9 = context.getBean(TestBean9.class);
		assertThat(testBean8.called1, is(true));
		assertThat(testBean8.called2, is(true));
		assertThat(testBean8.called3, is(true));
		assertThat(testBean9.called1, is(true));
		assertThat(testBean9.called2, is(true));
		assertThat(testBean9.called3, is(true));
		assertThat(testBean9.order1, isOneOf(0,1,2));
		assertThat(testBean9.order2, isOneOf(0,1,2));
		assertThat(testBean9.order3, isOneOf(0,1,2));
		assertThat(testBean8.order1, isOneOf(3,4,5));
		assertThat(testBean8.order2, isOneOf(3,4,5));
		assertThat(testBean8.order3, isOneOf(3,4,5));

		context.stop();
	}

	@Test
	public void testOrderingByMethod() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, OrderingByMethodConfig.class);
		DefaultYarnContainer container = context.getBean(DefaultYarnContainer.class);
		final StateWrapper stateWrapper = new StateWrapper();

		container.addContainerStateListener(new ContainerStateListener() {
			@Override
			public void state(ContainerState state, Object exit) {
				stateWrapper.state = state;
				stateWrapper.exit = exit;
			}
		});

		container.run();
		assertThat(stateWrapper.state, is(ContainerState.COMPLETED));
		assertThat(stateWrapper.exit, instanceOf(Integer.class));
		assertThat((Integer)stateWrapper.exit, is(0));

		TestBean10 testBean10 = context.getBean(TestBean10.class);
		assertThat(testBean10.called1, is(true));
		assertThat(testBean10.called2, is(true));
		assertThat(testBean10.called3, is(true));
		assertThat(testBean10.order2, is(0));
		assertThat(testBean10.order3, is(1));
		assertThat(testBean10.order1, is(2));

		context.stop();
	}

	@Test
	public void testOrderingByMixedClassAndMethod() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, OrderingByMixedClassAndMethodConfig.class);
		DefaultYarnContainer container = context.getBean(DefaultYarnContainer.class);
		final StateWrapper stateWrapper = new StateWrapper();

		container.addContainerStateListener(new ContainerStateListener() {
			@Override
			public void state(ContainerState state, Object exit) {
				stateWrapper.state = state;
				stateWrapper.exit = exit;
			}
		});

		container.run();
		assertThat(stateWrapper.state, is(ContainerState.COMPLETED));
		assertThat(stateWrapper.exit, instanceOf(List.class));
		assertThat((Integer)((List<?>)stateWrapper.exit).get(9), is(1));
		assertThat((Integer)((List<?>)stateWrapper.exit).get(11), is(2));

		TestBean8 testBean8 = context.getBean(TestBean8.class);
		TestBean9 testBean9 = context.getBean(TestBean9.class);
		TestBean10 testBean10 = context.getBean(TestBean10.class);
		TestBean11 testBean11 = context.getBean(TestBean11.class);
		assertThat(testBean8.called1, is(true));
		assertThat(testBean8.called2, is(true));
		assertThat(testBean8.called3, is(true));
		assertThat(testBean9.called1, is(true));
		assertThat(testBean9.called2, is(true));
		assertThat(testBean9.called3, is(true));
		assertThat(testBean10.called1, is(true));
		assertThat(testBean10.called2, is(true));
		assertThat(testBean10.called3, is(true));
		assertThat(testBean11.called1, is(true));
		assertThat(testBean11.called2, is(true));
		assertThat(testBean11.called3, is(true));

		assertThat(testBean8.order1, isOneOf(3,4,5));
		assertThat(testBean8.order2, isOneOf(3,4,5));
		assertThat(testBean8.order3, isOneOf(3,4,5));
		assertThat(testBean9.order1, isOneOf(0,1,2));
		assertThat(testBean9.order2, isOneOf(0,1,2));
		assertThat(testBean9.order3, isOneOf(0,1,2));
		assertThat(testBean10.order1, is(8));
		assertThat(testBean10.order2, is(6));
		assertThat(testBean10.order3, is(7));
		assertThat(testBean11.order1, is(10));
		assertThat(testBean11.order2, is(9));
		assertThat(testBean11.order3, is(11));

		context.stop();
	}

	@Test
	public void testFutureValues() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, FutureValuesConfig.class);
		DefaultYarnContainer container = context.getBean(DefaultYarnContainer.class);
		final StateWrapper stateWrapper = new StateWrapper();

		container.addContainerStateListener(new ContainerStateListener() {
			@Override
			public void state(ContainerState state, Object exit) {
				stateWrapper.state = state;
				stateWrapper.exit = exit;
			}
		});

		container.run();
		assertThat(stateWrapper.state, is(ContainerState.COMPLETED));
		assertThat(stateWrapper.exit, instanceOf(List.class));
		assertThat((Integer)((List<?>)stateWrapper.exit).get(0), is(1));
		assertThat((Integer)((List<?>)stateWrapper.exit).get(1), nullValue());
		assertThat((Integer)((List<?>)stateWrapper.exit).get(2), is(2));

		TestBean12 testBean12 = context.getBean(TestBean12.class);
		assertThat(testBean12.called1, is(true));
		assertThat(testBean12.called2, is(true));
		assertThat(testBean12.called3, is(true));

		assertThat(testBean12.order1, is(1));
		assertThat(testBean12.order2, is(0));
		assertThat(testBean12.order3, is(2));

		context.stop();
	}

	@Configuration
	static class BaseConfig {

		@Bean
		DefaultYarnContainer yarnContainer() {
			DefaultYarnContainer container = new DefaultYarnContainer();
			return container;
		}

		@Bean
		SpringYarnAnnotationPostProcessor springYarnAnnotationPostProcessor() {
			SpringYarnAnnotationPostProcessor post = new SpringYarnAnnotationPostProcessor();
			return post;
		}

	}

	@Configuration
	static class TestBean1Config {
		@Bean
		TestBean1 testBean() {
			return new TestBean1();
		}
	}

	@Configuration
	static class TestBean2Config {
		@Bean
		TestBean2 testBean() {
			return new TestBean2();
		}
	}

	@Configuration
	static class TestBean3Config {
		@Bean
		TestBean3 testBean() {
			return new TestBean3();
		}
	}

	@Configuration
	static class TestBean4Config {
		@Bean
		TestBean4 testBean() {
			return new TestBean4();
		}
	}

	@Configuration
	static class TestBean5Config {
		@Bean
		TestBean5 testBean() {
			return new TestBean5();
		}
	}

	@Configuration
	static class TestBean6Config {
		@Bean
		TestBean6 testBean() {
			return new TestBean6();
		}
	}

	@Configuration
	static class MultipleClassesConfig {
		@Bean
		TestBean1 testBean1() {
			return new TestBean1();
		}
		@Bean
		TestBean7 testBean7() {
			return new TestBean7();
		}
	}

	@Configuration
	static class MultipleMethodsConfig {
		@Bean
		TestBean5 testBean5() {
			return new TestBean5();
		}
	}

	@Configuration
	static class MultipleClassesAndMethodsConfig {
		@Bean
		TestBean1 testBean1() {
			return new TestBean1();
		}
		@Bean
		TestBean7 testBean7() {
			return new TestBean7();
		}
		@Bean
		TestBean5 testBean5() {
			return new TestBean5();
		}
	}

	@Configuration
	static class OrderingByClassConfig {
		private AtomicInteger executionOrder = new AtomicInteger();
		@Bean
		TestBean9 testBean9() {
			return new TestBean9(executionOrder);
		}
		@Bean
		TestBean8 testBean8() {
			return new TestBean8(executionOrder);
		}
	}

	@Configuration
	static class OrderingByMethodConfig {
		private AtomicInteger executionOrder = new AtomicInteger();
		@Bean
		TestBean10 testBean10() {
			return new TestBean10(executionOrder);
		}
	}

	@Configuration
	static class OrderingByMixedClassAndMethodConfig {
		private AtomicInteger executionOrder = new AtomicInteger();
		@Bean
		TestBean9 testBean9() {
			return new TestBean9(executionOrder);
		}
		@Bean
		TestBean8 testBean8() {
			return new TestBean8(executionOrder);
		}
		@Bean
		TestBean10 testBean10() {
			return new TestBean10(executionOrder);
		}
		@Bean
		TestBean11 testBean11() {
			return new TestBean11(executionOrder);
		}
	}

	@Configuration
	static class FutureValuesConfig {
		private AtomicInteger executionOrder = new AtomicInteger();
		@Bean
		TestBean12 testBean12() {
			return new TestBean12(executionOrder);
		}
	}

	@YarnComponent
	private static class TestBean1 {

		boolean called = false;

		@OnContainerStart
		public void test() {
			called = true;
		}
	}

	@YarnComponent
	private static class TestBean2 {

		@OnContainerStart
		public boolean test() {
			return true;
		}
	}

	@YarnComponent
	private static class TestBean3 {

		@OnContainerStart
		public int test() {
			return 10;
		}
	}

	@YarnComponent
	private static class TestBean4 {

		@OnContainerStart
		public void test() {
			throw new RuntimeException("failing");
		}
	}

	@YarnComponent
	private static class TestBean5 {

		boolean called1 = false;
		boolean called2 = false;

		@OnContainerStart
		public void test1() {
			called1 = true;
		}

		@OnContainerStart
		public void test2() {
			called2 = true;
		}
	}

	@YarnComponent
	private static class TestBean6 {

		@OnContainerStart
		public String test() {
			return ExitStatus.UNKNOWN.getExitCode();
		}
	}

	@YarnComponent
	private static class TestBean7 {

		boolean called = false;

		@OnContainerStart
		public void test() {
			called = true;
		}
	}

	@YarnComponent
	@Order(20)
	private static class TestBean8 {

		AtomicInteger executionOrder;
		boolean called1 = false;
		boolean called2 = false;
		boolean called3 = false;
		int order1 = -1;
		int order2 = -1;
		int order3 = -1;

		TestBean8(AtomicInteger executionOrder) {
			this.executionOrder = executionOrder;
		}

		@OnContainerStart
		public void test2() {
			called2 = true;
			order2 = executionOrder.getAndIncrement();
		}

		@OnContainerStart
		public void test1() {
			called1 = true;
			order1 = executionOrder.getAndIncrement();
		}

		@OnContainerStart
		public void test3() {
			called3 = true;
			order3 = executionOrder.getAndIncrement();
		}		// TestBean9 10

	}

	@YarnComponent
	@Order(10)
	private static class TestBean9 {

		AtomicInteger executionOrder;
		boolean called1 = false;
		boolean called2 = false;
		boolean called3 = false;
		int order1 = -1;
		int order2 = -1;
		int order3 = -1;

		TestBean9(AtomicInteger executionOrder) {
			this.executionOrder = executionOrder;
		}

		@OnContainerStart
		public void test1() {
			called1 = true;
			order1 = executionOrder.getAndIncrement();
		}

		@OnContainerStart
		public void test2() {
			called2 = true;
			order2 = executionOrder.getAndIncrement();
		}

		@OnContainerStart
		public void test3() {
			called3 = true;
			order3 = executionOrder.getAndIncrement();
		}
	}

	@YarnComponent
	private static class TestBean10 {

		AtomicInteger executionOrder;
		boolean called1 = false;
		boolean called2 = false;
		boolean called3 = false;
		int order1 = -1;
		int order2 = -1;
		int order3 = -1;

		TestBean10(AtomicInteger executionOrder) {
			this.executionOrder = executionOrder;
		}

		@OnContainerStart
		@Order(33)
		public void test1() {
			called1 = true;
			order1 = executionOrder.getAndIncrement();
		}

		@OnContainerStart
		@Order(31)
		public void test2() {
			called2 = true;
			order2 = executionOrder.getAndIncrement();
		}

		@OnContainerStart
		@Order(32)
		public void test3() {
			called3 = true;
			order3 = executionOrder.getAndIncrement();
		}
	}

	@YarnComponent
	private static class TestBean11 {

		AtomicInteger executionOrder;
		boolean called1 = false;
		boolean called2 = false;
		boolean called3 = false;
		int order1 = -1;
		int order2 = -1;
		int order3 = -1;

		TestBean11(AtomicInteger executionOrder) {
			this.executionOrder = executionOrder;
		}

		@OnContainerStart
		@Order(42)
		public void test1() {
			called1 = true;
			order1 = executionOrder.getAndIncrement();
		}

		@OnContainerStart
		@Order(41)
		public int test2() {
			called2 = true;
			order2 = executionOrder.getAndIncrement();
			return 1;
		}

		@OnContainerStart
		@Order(43)
		public int test3() {
			called3 = true;
			order3 = executionOrder.getAndIncrement();
			return 2;
		}
	}

	@YarnComponent
	private static class TestBean12 {

		AtomicInteger executionOrder;
		boolean called1 = false;
		boolean called2 = false;
		boolean called3 = false;
		int order1 = -1;
		int order2 = -1;
		int order3 = -1;

		TestBean12(AtomicInteger executionOrder) {
			this.executionOrder = executionOrder;
		}

		@OnContainerStart
		@Order(52)
		public void test1() {
			called1 = true;
			order1 = executionOrder.getAndIncrement();
		}

		@OnContainerStart
		@Order(51)
		public Future<Integer> test2() {
			called2 = true;
			order2 = executionOrder.getAndIncrement();
			return new AsyncResult<Integer>(1);
		}

		@OnContainerStart
		@Order(53)
		public Future<Integer> test3() {
			called3 = true;
			order3 = executionOrder.getAndIncrement();
			return new AsyncResult<Integer>(2);
		}
	}

	private static class StateWrapper {
		ContainerState state;
		Object exit;
	}

}
