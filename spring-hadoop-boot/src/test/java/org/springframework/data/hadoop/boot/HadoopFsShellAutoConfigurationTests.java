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
package org.springframework.data.hadoop.boot;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Test;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.hadoop.HadoopSystemConstants;

/**
 * Tests for {@link HadoopFsShellAutoConfiguration}.
 *
 * @author Janne Valkealahti
 *
 */
public class HadoopFsShellAutoConfigurationTests {

	private ConfigurableApplicationContext context;

	@After
	public void close() {
		if (context != null) {
			context.close();
		}
	}

	@Test
	public void testDefault() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		this.context = ctx;
		ctx.register(HadoopAutoConfiguration.class, HadoopFsShellAutoConfiguration.class);
		ctx.refresh();
		assertNotNull(context.getBean(HadoopSystemConstants.DEFAULT_ID_FSSHELL));
	}

	@Test
	public void testEnabled() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("spring.hadoop.fsshell.enabled=true").applyTo(ctx);
		this.context = ctx;
		ctx.register(HadoopAutoConfiguration.class, HadoopFsShellAutoConfiguration.class);
		ctx.refresh();
		assertNotNull(context.getBean(HadoopSystemConstants.DEFAULT_ID_FSSHELL));
	}

	@Test
	public void testDisabled() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("spring.hadoop.fsshell.enabled=false").applyTo(ctx);
		this.context = ctx;
		ctx.register(HadoopAutoConfiguration.class, HadoopFsShellAutoConfiguration.class);
		ctx.refresh();
		assertThat(context.containsBean(HadoopSystemConstants.DEFAULT_ID_FSSHELL), is(false));
	}

}
