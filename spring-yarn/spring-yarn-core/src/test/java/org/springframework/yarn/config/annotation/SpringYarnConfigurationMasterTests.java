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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.yarn.TestUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnConfigConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerConfigurer;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.ResourceLocalizer;
import org.springframework.yarn.support.YarnContextUtils;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
public class SpringYarnConfigurationMasterTests {

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testSimpleConfig() throws Exception {
		assertNotNull(ctx);
		assertTrue(ctx.containsBean("yarnConfiguration"));
		YarnConfiguration config = (YarnConfiguration) ctx.getBean("yarnConfiguration");
		assertNotNull(config);

		YarnAppmaster master = (YarnAppmaster) ctx.getBean("yarnAppmaster");
		assertNotNull(master);

		assertTrue(ctx.containsBean("yarnLocalresources"));
		ResourceLocalizer localizer = (ResourceLocalizer) ctx.getBean("yarnLocalresources");
		assertNotNull(localizer);

		assertTrue(ctx.containsBean(YarnSystemConstants.DEFAULT_ID_ENVIRONMENT));
		@SuppressWarnings("unchecked")
		Map<String, String> environment = (Map<String, String>) ctx.getBean(YarnSystemConstants.DEFAULT_ID_ENVIRONMENT);
		assertNotNull(environment);

		assertTrue(ctx.containsBean(YarnContextUtils.TASK_SCHEDULER_BEAN_NAME));
		assertNotNull(ctx.getBean(YarnContextUtils.TASK_SCHEDULER_BEAN_NAME));

		assertThat(config.get("resource.property"), is("test-site-1.xml"));
		assertThat(config.get("resource.property.2"), is("test-site-2.xml"));
		assertThat(config.get("foo"), is("jee"));
		assertThat(config.get("fs.defaultFS"), is("hdfs://foo.uri"));

		Collection<CopyEntry> copyEntries = TestUtils.readField("copyEntries", localizer);
		assertNotNull(copyEntries);
		assertThat(copyEntries.size(), is(2));

		Iterator<CopyEntry> iterator = copyEntries.iterator();

		CopyEntry copyEntry1 = iterator.next();
		String copyEntrySrc1 = TestUtils.readField("src", copyEntry1);
		String copyEntryDest1 = TestUtils.readField("dest", copyEntry1);
		Boolean copyEntryStaging1 = TestUtils.readField("staging", copyEntry1);
		assertThat(copyEntrySrc1, is("foo.jar"));
		assertThat(copyEntryDest1, is("/tmp"));
		assertThat(copyEntryStaging1, is(true));

		CopyEntry copyEntry2 = iterator.next();
		String copyEntrySrc2 = TestUtils.readField("src", copyEntry2);
		String copyEntryDest2 = TestUtils.readField("dest", copyEntry2);
		Boolean copyEntryStaging2 = TestUtils.readField("staging", copyEntry2);
		assertThat(copyEntrySrc2, is("foo2.jar"));
		assertThat(copyEntryDest2, is("/tmp"));
		assertThat(copyEntryStaging2, is(false));

	}

	@Configuration
	@EnableYarn(enable=Enable.APPMASTER)
	static class Config extends SpringYarnConfigurerAdapter {

		@Override
		public void configure(YarnConfigConfigurer config) throws Exception {
			config
				.fileSystemUri("hdfs://foo.uri")
				.withResources()
					.resource("classpath:/test-site-1.xml")
					.resource("classpath:/test-site-2.xml")
					.and()
				.withProperties()
					.property("foo", "jee");
		}

		@Override
		public void configure(YarnResourceLocalizerConfigurer localizer) throws Exception {
			localizer
				.withCopy()
					.copy("foo.jar", "/tmp", true)
					.source("foo2.jar").destination("/tmp").staging(false);
		}

		@Override
		public void configure(YarnEnvironmentConfigurer environment) throws Exception {
			environment
				.entry("CONTAINER_ID", "container_1360089121174_0011_01_000001");
		}

		// TODO: need to fake appmaster for it to not try to connect hadoop
		@Override
		public void configure(YarnAppmasterConfigurer master) throws Exception {
			Properties properties = new Properties();
			properties.put("foo1", "jee1");

			master
				.withContainerRunner()
					.stdout("stdout")
					.stderr("stderr")
					.arguments(properties)
					.argument("foo2", "jee2");
		}

	}

}
