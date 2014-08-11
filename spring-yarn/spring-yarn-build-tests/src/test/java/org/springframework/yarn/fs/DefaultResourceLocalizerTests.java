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
package org.springframework.yarn.fs;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.RawCopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.test.context.MiniYarnCluster;
import org.springframework.yarn.test.context.YarnDelegatingSmartContextLoader;

/**
 * Tests for {@link DefaultResourceLocalizer}.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster
@DirtiesContext(classMode=ClassMode.AFTER_CLASS)
public class DefaultResourceLocalizerTests {

	@Autowired
	private Configuration configuration;

	@Test
	public void testSimpleCopy() throws Exception {
		String dir = "/DefaultResourceLocalizerTests-testSimpleCopy";
		LocalResourcesFactoryBean factory = new LocalResourcesFactoryBean();
		factory.setConfiguration(configuration);

		List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
		CopyEntry entry = new CopyEntry("classpath:/test-site-1.xml", dir, false);
		copyEntries.add(entry);
		factory.setCopyEntries(copyEntries);
		factory.setHdfsEntries(new ArrayList<LocalResourcesFactoryBean.TransferEntry>());
		factory.afterPropertiesSet();

		SmartResourceLocalizer localizer = (SmartResourceLocalizer) factory.getObject();
		localizer.copy();

		FileSystem fs = FileSystem.get(configuration);
		FileStatus fileStatus = fs.getFileStatus(new Path(dir + "/test-site-1.xml"));
		assertThat(fileStatus.isFile(), is(true));
		assertThat(fileStatus.getLen(), greaterThan(0l));
	}

	@Test
	public void testSimpleCopyHomeStaging() throws Exception {
		String dir = "DefaultResourceLocalizerTests-testSimpleCopyHomeStaging";
		LocalResourcesFactoryBean factory = new LocalResourcesFactoryBean();
		factory.setConfiguration(configuration);

		List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
		CopyEntry entry = new CopyEntry("classpath:/test-site-1.xml", null, true);
		copyEntries.add(entry);
		factory.setCopyEntries(copyEntries);
		factory.setHdfsEntries(new ArrayList<LocalResourcesFactoryBean.TransferEntry>());
		factory.afterPropertiesSet();

		SmartResourceLocalizer localizer = (SmartResourceLocalizer) factory.getObject();
		localizer.setStagingDirectory(new Path(dir));
		localizer.setStagingId("foo-id");
		localizer.copy();

		FileSystem fs = FileSystem.get(configuration);
		FileStatus fileStatus = fs.getFileStatus(new Path(dir + "/foo-id/test-site-1.xml"));
		assertThat(fileStatus.isFile(), is(true));
		assertThat(fileStatus.getLen(), greaterThan(0l));
	}

	@Test
	public void testSimpleDistribute() throws Exception {
		String dir = "/DefaultResourceLocalizerTests-testSimpleDistribute";
		LocalResourcesFactoryBean factory = new LocalResourcesFactoryBean();
		factory.setConfiguration(configuration);

		List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
		CopyEntry entry = new CopyEntry("classpath:/test-site-1.xml", dir, false);
		copyEntries.add(entry);
		factory.setCopyEntries(copyEntries);

		List<TransferEntry> transferEntries = new ArrayList<TransferEntry>();
		TransferEntry tEntry = new TransferEntry(null, null, dir + "/test-site-1.xml", false);
		transferEntries.add(tEntry);
		factory.setHdfsEntries(transferEntries);

		factory.afterPropertiesSet();
		SmartResourceLocalizer localizer = (SmartResourceLocalizer) factory.getObject();

		localizer.distribute();

		Map<String, LocalResource> resources = localizer.getResources();
		assertThat(resources, notNullValue());
		assertThat(resources.size(), is(1));
	}

	@Test
	public void testDistributeGlobalCustomStaging() throws Exception {
		LocalResourcesFactoryBean factory = new LocalResourcesFactoryBean();
		factory.setConfiguration(configuration);

		List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
		CopyEntry entry = new CopyEntry("classpath:/test-site-1.xml", null, true);
		copyEntries.add(entry);
		factory.setCopyEntries(copyEntries);

		List<TransferEntry> transferEntries = new ArrayList<TransferEntry>();
		TransferEntry tEntry = new TransferEntry(null, null, "/test-site-1.xml", true);
		transferEntries.add(tEntry);
		factory.setHdfsEntries(transferEntries);

		factory.afterPropertiesSet();
		SmartResourceLocalizer localizer = (SmartResourceLocalizer) factory.getObject();

		localizer.setStagingDirectory(new Path("/tmp/foo"));
		localizer.setStagingId("foo-id");
		localizer.distribute();

		Map<String, LocalResource> resources = localizer.getResources();
		assertThat(resources, notNullValue());
		assertThat(resources.size(), is(1));

		FileSystem fs = FileSystem.get(configuration);
		FileStatus fileStatus = fs.getFileStatus(new Path("/tmp/foo/foo-id/test-site-1.xml"));
		assertThat(fileStatus.isFile(), is(true));
		assertThat(fileStatus.getLen(), greaterThan(0l));

	}

	@Test
	public void testDistributeGlobalDefaultStaging() throws Exception {
		LocalResourcesFactoryBean factory = new LocalResourcesFactoryBean();
		factory.setConfiguration(configuration);

		List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
		CopyEntry entry = new CopyEntry("classpath:/test-site-1.xml", null, true);
		copyEntries.add(entry);
		factory.setCopyEntries(copyEntries);

		List<TransferEntry> transferEntries = new ArrayList<TransferEntry>();
		TransferEntry tEntry = new TransferEntry(null, null, "/test-site-1.xml", true);
		transferEntries.add(tEntry);
		factory.setHdfsEntries(transferEntries);
		factory.afterPropertiesSet();

		SmartResourceLocalizer localizer = (SmartResourceLocalizer) factory.getObject();

		localizer.setStagingId("foo-id");

		localizer.distribute();

		Map<String, LocalResource> resources = localizer.getResources();
		assertThat(resources, notNullValue());
		assertThat(resources.size(), is(1));

		FileSystem fs = FileSystem.get(configuration);
		FileStatus fileStatus = fs.getFileStatus(new Path("/syarn/staging/foo-id/test-site-1.xml"));
		assertThat(fileStatus.isFile(), is(true));
		assertThat(fileStatus.getLen(), greaterThan(0l));
	}

	@Test
	public void testDistributeHomeStaging() throws Exception {
		String dir = "DefaultResourceLocalizerTests-testDistributeHomeStaging";
		LocalResourcesFactoryBean factory = new LocalResourcesFactoryBean();
		factory.setConfiguration(configuration);

		List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
		CopyEntry entry = new CopyEntry("classpath:/test-site-1.xml", null, true);
		copyEntries.add(entry);
		factory.setCopyEntries(copyEntries);

		List<TransferEntry> transferEntries = new ArrayList<TransferEntry>();
		TransferEntry tEntry = new TransferEntry(null, null, "/test-site-1.xml", true);
		transferEntries.add(tEntry);
		factory.setHdfsEntries(transferEntries);
		factory.afterPropertiesSet();

		SmartResourceLocalizer localizer = (SmartResourceLocalizer) factory.getObject();

		localizer.setStagingDirectory(new Path(dir));
		localizer.setStagingId("foo-id");
		localizer.distribute();

		Map<String, LocalResource> resources = localizer.getResources();
		assertThat(resources, notNullValue());
		assertThat(resources.size(), is(1));


		FileSystem fs = FileSystem.get(configuration);
		FileStatus fileStatus = fs.getFileStatus(new Path(dir + "/foo-id/test-site-1.xml"));
		assertThat(fileStatus.isFile(), is(true));
		assertThat(fileStatus.getLen(), greaterThan(0l));

	}

	@Test
	public void testRawEntries() throws Exception {
		String dir = "/DefaultResourceLocalizerTests-testRawEntries";
		LocalResourcesFactoryBean factory = new LocalResourcesFactoryBean();
		factory.setConfiguration(configuration);

		List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
		factory.setCopyEntries(copyEntries);
		factory.setHdfsEntries(new ArrayList<LocalResourcesFactoryBean.TransferEntry>());

		List<RawCopyEntry> rawEntries = new ArrayList<RawCopyEntry>();
		rawEntries.add(new RawCopyEntry(new byte[10], dir + "/rawContent1", false));
		factory.setRawCopyEntries(rawEntries);
		factory.afterPropertiesSet();

		SmartResourceLocalizer localizer = (SmartResourceLocalizer) factory.getObject();
		localizer.copy();

		FileSystem fs = FileSystem.get(configuration);
		FileStatus fileStatus = fs.getFileStatus(new Path(dir + "/rawContent1"));
		assertThat(fileStatus.isFile(), is(true));
		assertThat(fileStatus.getLen(), is(10l));
	}

	@Test
	public void testRawEntriesStaging() throws Exception {
		String dir = "DefaultResourceLocalizerTests-testRawEntriesStaging";
		LocalResourcesFactoryBean factory = new LocalResourcesFactoryBean();
		factory.setConfiguration(configuration);

		List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
		factory.setCopyEntries(copyEntries);
		factory.setHdfsEntries(new ArrayList<LocalResourcesFactoryBean.TransferEntry>());

		List<RawCopyEntry> rawEntries = new ArrayList<RawCopyEntry>();
		rawEntries.add(new RawCopyEntry(new byte[10], "rawContent1", true));
		factory.setRawCopyEntries(rawEntries);
		factory.afterPropertiesSet();

		SmartResourceLocalizer localizer = (SmartResourceLocalizer) factory.getObject();
		localizer.setStagingDirectory(new Path(dir));
		localizer.setStagingId("foo-id");
		localizer.copy();

		FileSystem fs = FileSystem.get(configuration);
		FileStatus fileStatus = fs.getFileStatus(new Path(dir + "/foo-id/rawContent1"));
		assertThat(fileStatus.isFile(), is(true));
		assertThat(fileStatus.getLen(), is(10l));
	}

	@org.springframework.context.annotation.Configuration
	public static class Config {

	}

}
