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
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigurer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
public class PropertiesEnvironmentAnnotationTests {

	@Autowired
	private ApplicationContext ctx;

	@Resource(name = YarnSystemConstants.DEFAULT_ID_ENVIRONMENT)
	private Map<String, String> defaultEnvironment;

	@Test
	public void testPropertiesEnvironment() throws Exception {
		assertThat(defaultEnvironment, notNullValue());
		assertThat(defaultEnvironment.get("foo20"), is("jee20"));
		assertThat(defaultEnvironment.get("foo21"), is("jee21"));
		assertThat(defaultEnvironment.get("foo22"), is("jee22"));
		assertThat(defaultEnvironment.get("foo23"), is("jee23"));
		assertThat(defaultEnvironment.get("foo24"), is("jee24"));
		assertThat(defaultEnvironment.get("foo25"), is("jee25"));
		assertThat(defaultEnvironment.get("foo26"), is("jee26"));
		assertThat(defaultEnvironment.get("foo27"), is("jee27"));
		assertThat(defaultEnvironment.get("foo28"), is("jee28"));
	}

	@Configuration
	@EnableYarn(enable=Enable.BASE)
	static class Config extends SpringYarnConfigurerAdapter {

		@Bean(name="props")
		public Properties getProperties() throws IOException {
			PropertiesFactoryBean fb = new PropertiesFactoryBean();
			fb.setLocation(new ClassPathResource("props.properties"));
			fb.afterPropertiesSet();
			return fb.getObject();
		}

		@Override
		public void configure(YarnEnvironmentConfigurer environment) throws Exception {
				environment
					.propertiesLocation("cfg-1.properties","cfg-2.properties")
					.withProperties()
						.properties(getProperties())
						.property("key1", "value1")
						.and()
					.entry("foo26", "jee26")
					.entry("foo27", "jee27")
					.entry("foo28", "jee28");
		}

	}

}
