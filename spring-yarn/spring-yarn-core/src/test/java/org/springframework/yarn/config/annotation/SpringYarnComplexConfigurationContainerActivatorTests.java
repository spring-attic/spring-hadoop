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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.annotation.YarnComponent;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.container.DefaultYarnContainer;
import org.springframework.yarn.container.YarnContainer;

/**
 * Tests verifying container annotation config creating
 * needed components and logic of pojo method execution
 * happens correctly.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
public class SpringYarnComplexConfigurationContainerActivatorTests {

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testSimpleConfig() throws Exception {
		assertNotNull(ctx);
		assertTrue(ctx.containsBean(YarnSystemConstants.DEFAULT_ID_CONTAINER));
		assertTrue(ctx.containsBean("org.springframework.yarn.internal.springYarnAnnotationPostProcessor"));
		YarnContainer container = ctx.getBean(YarnSystemConstants.DEFAULT_ID_CONTAINER, YarnContainer.class);
		assertTrue(container instanceof DefaultYarnContainer);
		assertNotNull(container);
		container.run();
		TestContainerPojo1 testContainerPojo1 = ctx.getBean(TestContainerPojo1.class);
		assertTrue(testContainerPojo1.executed);
		TestContainerPojo2 testContainerPojo2 = ctx.getBean(TestContainerPojo2.class);
		assertTrue(testContainerPojo2.executed);
	}

	@Configuration
	@EnableYarn(enable=Enable.CONTAINER)
	static class Config extends SpringYarnConfigurerAdapter {

		@Bean
		TestContainerPojo1 testContainerPojo1() {
			return new TestContainerPojo1();
		}

		@Bean
		TestContainerPojo2 testContainerPojo2() {
			return new TestContainerPojo2();
		}

	}

	@YarnComponent
	public static class TestContainerPojo1 {
		boolean executed;

		@OnContainerStart
		public void doSomething() {
			executed = true;
		}

	}

	@YarnComponent
	public static class TestContainerPojo2 {
		boolean executed;

		@OnContainerStart
		public void doSomething() {
			executed = true;
		}

	}

}
