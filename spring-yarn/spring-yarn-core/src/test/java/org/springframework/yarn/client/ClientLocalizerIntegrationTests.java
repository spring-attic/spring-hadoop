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
package org.springframework.yarn.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Resource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.URL;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.fs.ResourceLocalizer;
import org.springframework.yarn.fs.SmartResourceLocalizer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@Ignore
public class ClientLocalizerIntegrationTests {

	@Resource(name = "yarnClient")
	private YarnClient client;

	@Resource(name = "yarnLocalresources")
	private ResourceLocalizer localizer;

	@Resource(name = "yarnConfiguration")
	private Configuration configuration;

	private FileSystem fs;

	@Before
	public void setUp() throws IOException {
		fs = FileSystem.get(configuration);
		fs.mkdirs(new Path("/syarn-tmp"));
	}

	@Test
	public void testDistributeWithCopy() throws IOException {
		assertThat(client, notNullValue());

		((SmartResourceLocalizer)localizer).setStagingDirectory(new Path("/syarn-tmp/ClientLocalizerIntegrationTests/1"));
		((SmartResourceLocalizer)localizer).distribute();

		FileStatus fileStatus = fs.getFileStatus(new Path("/syarn-tmp/ClientLocalizerIntegrationTests/1/hadoop.properties"));
		assertThat(fileStatus, notNullValue());
		assertThat(fileStatus.isFile(), is(true));

		Map<String, LocalResource> resources = localizer.getResources();
		assertThat(resources, notNullValue());
		LocalResource localResource = resources.get("hadoop.properties");
		assertThat(localResource, notNullValue());
		URL resource = localResource.getResource();
		assertThat(resource, notNullValue());
		assertThat(resource.getFile(), is("/syarn-tmp/ClientLocalizerIntegrationTests/1/hadoop.properties"));
	}

}
