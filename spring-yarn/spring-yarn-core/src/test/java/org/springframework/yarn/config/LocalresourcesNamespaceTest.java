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
package org.springframework.yarn.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.TestUtils;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

/**
 * Namespace tests for yarn:localresources elements.
 * <p>
 * Tests only on factory and plain bean level meaning
 * resources inside ResourceLocalizer cannot be tested
 * because it would involve communication with hdfs. There
 * are separate tests for those.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration("/org/springframework/yarn/config/localresources-ns.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class LocalresourcesNamespaceTest {

	@Resource(name = "yarnLocalresources")
	private ResourceLocalizer defaultResources;

	@Resource(name = "&yarnLocalresources")
	private LocalResourcesFactoryBean defaultLocalResourcesFactoryBean;

	@Resource(name = "yarnLocalresourcesWithDefaults")
	private ResourceLocalizer resourcesWithGlobal;

	@Resource(name = "&yarnLocalresourcesWithDefaults")
	private LocalResourcesFactoryBean localResourcesFactoryBeanWithGlobal;

	@Resource(name = "yarnLocalresourcesWithMixed")
	private ResourceLocalizer resourcesWithMixed;

	@Resource(name = "&yarnLocalresourcesWithMixed")
	private LocalResourcesFactoryBean localResourcesFactoryBeanWithMixed;

	@Resource(name = "&yarnLocalresourcesOverride")
	private LocalResourcesFactoryBean localResourcesFactoryBeanOverride;

	/**
	 * When only single path is defined configuration should assume
	 * it is accessed from local hdfs access point and other settings
	 * set to defaults.
	 */
	@Test
	public void testDefaultsWithMinimalSetup() throws Exception {
		assertNotNull(defaultResources);
		assertNotNull(defaultLocalResourcesFactoryBean);

		Configuration configuration = TestUtils.readField("configuration", defaultLocalResourcesFactoryBean);
		assertNotNull(configuration);

		//should be something like fs.defaultFS=hdfs://x.x.x.x:9000
		String defaultFs = configuration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
		assertThat(defaultFs, startsWith("hdfs"));
		assertThat(defaultFs, endsWith("8020"));

		Collection<TransferEntry> hdfsEntries = TestUtils.readField("hdfsEntries", defaultLocalResourcesFactoryBean);
		assertNotNull(hdfsEntries);
		assertThat(hdfsEntries.size(), is(1));

		TransferEntry entry = (TransferEntry) hdfsEntries.toArray()[0];
		String path = TestUtils.readField("path", entry);
		assertThat(path, is("/tmp/foo.jar"));
		LocalResourceType type = TestUtils.readField("type", entry);
		assertThat(type, is(LocalResourceType.FILE));
		LocalResourceVisibility visibility = TestUtils.readField("visibility", entry);
		assertThat(visibility, is(LocalResourceVisibility.APPLICATION));
	}

	@Test
	public void testGlobalDefaults() throws Exception {
		assertNotNull(resourcesWithGlobal);
		assertNotNull(localResourcesFactoryBeanWithGlobal);
		Configuration configuration = TestUtils.readField("configuration", localResourcesFactoryBeanWithGlobal);
		assertNotNull(configuration);

		Collection<TransferEntry> hdfsEntries = TestUtils.readField("hdfsEntries", localResourcesFactoryBeanWithGlobal);
		assertNotNull(hdfsEntries);
		assertThat(hdfsEntries.size(), is(1));

		TransferEntry entry = (TransferEntry) hdfsEntries.toArray()[0];
		String path = TestUtils.readField("path", entry);
		assertThat(path, is("/tmp/foo.jar"));
		LocalResourceType type = TestUtils.readField("type", entry);
		assertThat(type, is(LocalResourceType.ARCHIVE));
		LocalResourceVisibility visibility = TestUtils.readField("visibility", entry);
		assertThat(visibility, is(LocalResourceVisibility.PRIVATE));
	}

	@Test
	public void testGlobalLocalMixed() throws Exception {
		assertNotNull(resourcesWithMixed);
		assertNotNull(localResourcesFactoryBeanWithMixed);
		Configuration configuration = TestUtils.readField("configuration", localResourcesFactoryBeanWithMixed);
		assertNotNull(configuration);

		Collection<TransferEntry> hdfsEntries = TestUtils.readField("hdfsEntries", localResourcesFactoryBeanWithMixed);
		assertNotNull(hdfsEntries);
		assertThat(hdfsEntries.size(), is(3));

		TransferEntry entry = (TransferEntry) hdfsEntries.toArray()[0];
		String path = TestUtils.readField("path", entry);
		assertThat(path, is("/tmp/foo.jar"));
		LocalResourceType type = TestUtils.readField("type", entry);
		assertThat(type, is(LocalResourceType.FILE));
		LocalResourceVisibility visibility = TestUtils.readField("visibility", entry);
		assertThat(visibility, is(LocalResourceVisibility.PUBLIC));

		entry = (TransferEntry) hdfsEntries.toArray()[1];
		path = TestUtils.readField("path", entry);
		assertThat(path, is("/tmp/jee.jar"));
		type = TestUtils.readField("type", entry);
		assertThat(type, is(LocalResourceType.FILE));
		visibility = TestUtils.readField("visibility", entry);
		assertThat(visibility, is(LocalResourceVisibility.PUBLIC));

		entry = (TransferEntry) hdfsEntries.toArray()[2];
		path = TestUtils.readField("path", entry);
		assertThat(path, is("/tmp/bar.jar"));
		type = TestUtils.readField("type", entry);
		assertThat(type, is(LocalResourceType.ARCHIVE));
		visibility = TestUtils.readField("visibility", entry);
		assertThat(visibility, is(LocalResourceVisibility.PRIVATE));
	}

	@Test
	public void testGlobalLocalOverride() throws Exception {
		assertNotNull(localResourcesFactoryBeanOverride);
	}

}
