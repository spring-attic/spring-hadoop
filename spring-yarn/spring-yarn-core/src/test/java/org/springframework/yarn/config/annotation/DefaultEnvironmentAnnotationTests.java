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
package org.springframework.yarn.config.annotation;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Map;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigurer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
public class DefaultEnvironmentAnnotationTests {

	@Autowired
	private ApplicationContext ctx;

	@Resource(name = YarnSystemConstants.DEFAULT_ID_ENVIRONMENT)
	private Map<String, String> defaultEnvironment;

	@Test
	public void testDefaultEnvironment() throws Exception {
		assertThat(defaultEnvironment, notNullValue());
		assertThat(defaultEnvironment.get("foo"), is("jee"));
		assertThat(defaultEnvironment.get("test.foo.2"), nullValue());
		assertThat(defaultEnvironment.get("myvar1"), is("myvalue1"));
	}

	@Configuration
	@EnableYarn(enable=Enable.BASE)
	static class Config extends SpringYarnConfigurerAdapter {

		@Override
		public void configure(YarnEnvironmentConfigurer environment) throws Exception {
				environment
					.entry("myvar1", "myvalue1")
					.entry("foo", "jee");
		}

	}

}
