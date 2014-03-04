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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.yarn.annotation.OnYarnContainerStart;
import org.springframework.yarn.annotation.YarnContainer;
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
	public void testContainerBeanThrowsExceptionTwoAnnotations() {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(BaseConfig.class, TestBean5Config.class);
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

	@YarnContainer
	private static class TestBean1 {

		@OnYarnContainerStart
		public void test() {
		}
	}

	@YarnContainer
	private static class TestBean2 {

		@OnYarnContainerStart
		public boolean test() {
			return true;
		}
	}

	@YarnContainer
	private static class TestBean3 {

		@OnYarnContainerStart
		public int test() {
			return 10;
		}
	}

	@YarnContainer
	private static class TestBean4 {

		@OnYarnContainerStart
		public void test() {
			throw new RuntimeException("failing");
		}
	}

	@YarnContainer
	private static class TestBean5 {

		@OnYarnContainerStart
		public void test1() {
		}

		@OnYarnContainerStart
		public void test2() {
		}
	}

	@YarnContainer
	private static class TestBean6 {

		@OnYarnContainerStart
		public String test() {
			return ExitStatus.UNKNOWN.getExitCode();
		}
	}

	private static class StateWrapper {
		ContainerState state;
		Object exit;
	}

}
