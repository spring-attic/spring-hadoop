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
package org.springframework.yarn.config.annotation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.annotation.YarnComponent;
import org.springframework.yarn.container.ContainerHandler;

/**
 * Tests for post processing pojos to create ContainerHandlers.
 * Mostly verifying usage of {@link SpringYarnAnnotationPostProcessor}.
 *
 * @author Janne Valkealahti
 *
 */
public class SpringYarnAnnotationPostProcessorTests {

	@Test
	public void testSimpleOnContainerStart() {
		@SuppressWarnings("resource")
		GenericApplicationContext context = new GenericApplicationContext();
		SpringYarnAnnotationPostProcessor postProcessor = new SpringYarnAnnotationPostProcessor();
		postProcessor.setBeanFactory(context.getBeanFactory());
		postProcessor.afterPropertiesSet();

		TestBean1 testBean1 = new TestBean1();
		postProcessor.postProcessAfterInitialization(testBean1, "testBean1");
		context.refresh();

		assertTrue(context.containsBean("testBean1.test1.onContainerStart"));
		Object endpoint = context.getBean("testBean1.test1.onContainerStart");
		assertTrue(endpoint instanceof ContainerHandler);

		assertNotNull(context.getBean(ContainerHandler.class));

		context.stop();
	}

	@Test
	public void testComplexOnContainerStart() {
		@SuppressWarnings("resource")
		GenericApplicationContext context = new GenericApplicationContext();
		SpringYarnAnnotationPostProcessor postProcessor = new SpringYarnAnnotationPostProcessor();
		postProcessor.setBeanFactory(context.getBeanFactory());
		postProcessor.afterPropertiesSet();

		TestBean1 testBean1 = new TestBean1();
		postProcessor.postProcessAfterInitialization(testBean1, "testBean1");
		TestBean2 testBean2 = new TestBean2();
		postProcessor.postProcessAfterInitialization(testBean2, "testBean2");
		context.refresh();

		assertTrue(context.containsBean("testBean1.test1.onContainerStart"));
		Object endpoint = context.getBean("testBean1.test1.onContainerStart");
		assertTrue(endpoint instanceof ContainerHandler);
		assertTrue(context.containsBean("testBean2.test2.onContainerStart"));
		endpoint = context.getBean("testBean2.test2.onContainerStart");
		assertTrue(endpoint instanceof ContainerHandler);

		Map<String, ContainerHandler> beans = context.getBeansOfType(ContainerHandler.class);
		assertThat(beans.size(), is(2));

		context.stop();
	}

	@YarnComponent
	private static class TestBean1 {

		@OnContainerStart
		public void test1() {
		}
	}

	@YarnComponent
	private static class TestBean2 {

		@OnContainerStart
		public void test2() {
		}
	}

}
