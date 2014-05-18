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

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.annotation.YarnComponent;
import org.springframework.yarn.container.ContainerHandler;

/**
 * Tests for post processing pojos to create ContainerHandlers.
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
		TestBean testBean = new TestBean();
		postProcessor.postProcessAfterInitialization(testBean, "testBean");
		context.refresh();
		assertTrue(context.containsBean("testBean.test.onContainerStart"));
		Object endpoint = context.getBean("testBean.test.onContainerStart");
		assertTrue(endpoint instanceof ContainerHandler);
		context.stop();
	}

	@YarnComponent
	private static class TestBean {

		@OnContainerStart
		public void test() {
		}
	}

}
