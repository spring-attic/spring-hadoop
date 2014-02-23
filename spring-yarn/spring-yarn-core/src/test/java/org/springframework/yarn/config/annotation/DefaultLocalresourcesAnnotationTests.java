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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;

import java.util.Collection;

import javax.annotation.Resource;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
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
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.builders.YarnConfigConfigurer;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerConfigurer;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
public class DefaultLocalresourcesAnnotationTests {

	@Autowired
	private ApplicationContext ctx;

	@Resource(name = YarnSystemConstants.DEFAULT_ID_CONFIGURATION)
	private YarnConfiguration yarnConfiguration;

	@Resource(name = YarnSystemConstants.DEFAULT_ID_LOCAL_RESOURCES)
	private ResourceLocalizer resourceLocalizer;

	@Test
	public void testDefaultConfig() throws Exception {
		assertThat(resourceLocalizer, notNullValue());
		String defaultFs = yarnConfiguration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
		assertThat(defaultFs, startsWith("hdfs"));
		assertThat(defaultFs, endsWith("8020"));

		Collection<TransferEntry> hdfsEntries = TestUtils.readField("transferEntries", resourceLocalizer);
		assertThat(hdfsEntries, notNullValue());
		assertThat(hdfsEntries.size(), is(1));

		TransferEntry entry = (TransferEntry) hdfsEntries.toArray()[0];
		String path = TestUtils.readField("path", entry);
		assertThat(path, is("/tmp/foo.jar"));
		LocalResourceType type = TestUtils.readField("type", entry);
		assertThat(type, is(LocalResourceType.FILE));
		LocalResourceVisibility visibility = TestUtils.readField("visibility", entry);
		assertThat(visibility, is(LocalResourceVisibility.APPLICATION));
	}

	@Configuration
	@EnableYarn(enable=Enable.BASE)
	static class Config extends SpringYarnConfigurerAdapter {

		@Override
		public void configure(YarnConfigConfigurer config) throws Exception {
			config
				.fileSystemUri("hdfs://localhost:8020")
				.resourceManagerAddress("10.10.10.10:8032");
		}

		@Override
		public void configure(YarnResourceLocalizerConfigurer localizer) throws Exception {
			localizer
			.withCopy()
				.copy("foo.jar", "/tmp", true)
				.and()
			.withHdfs()
				.hdfs("/tmp/foo.jar");
		}

	}

}
