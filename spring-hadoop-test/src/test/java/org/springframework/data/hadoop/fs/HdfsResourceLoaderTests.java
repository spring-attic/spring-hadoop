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
package org.springframework.data.hadoop.fs;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.HadoopSystemConstants;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test for interacting with Hadoop HDFS with {@code HdfsResourceLoader}.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HdfsResourceLoaderTests {

	@javax.annotation.Resource(name = HadoopSystemConstants.DEFAULT_ID_RESOURCE_LOADER)
	private HdfsResourceLoader loader;

	@javax.annotation.Resource(name = "loaderWithUser")
	private HdfsResourceLoader loaderWithUser;

	@Test
	public void testFilesWithDifferentUsers() throws Exception {
		String userFileName1 = "HdfsResourceLoaderTests-testFilesWithDifferentUsers1.txt";
		String userFileName2 = "HdfsResourceLoaderTests-testFilesWithDifferentUsers2.txt";

		// files should end up on different user directories
		TestUtils.writeToFS(loader, userFileName1);
		TestUtils.writeToFS(loaderWithUser, userFileName2);

		assertFileViaLoader(loader, userFileName1, true);
		assertFileViaLoader(loader, userFileName2, false);
		assertFileViaLoader(loaderWithUser, userFileName1, false);
		assertFileViaLoader(loaderWithUser, userFileName2, true);
	}

	@Test
	public void testFilesWithPaths() throws Exception {
		String fileName1 = "HdfsResourceLoaderTests-testFilesWithPaths1.txt";

		TestUtils.writeToFS(loader, "/test/" + fileName1);

		assertFileViaLoader(loader, "/test/" + fileName1, true);
	}

	@Test
	public void testFilesWithTilde() throws Exception {
		String userFileName1 = "HdfsResourceLoaderTests-testFilesWithTilde1.txt";

		TestUtils.writeToFS(loader, userFileName1);

		assertFileViaLoaderWithPatter(loader, "~/" + userFileName1 + "*", true);
		assertFileViaLoader(loader, "~/" + userFileName1, true);
	}

	private static void assertFileViaLoader(HdfsResourceLoader loader, String path, boolean shouldExist) throws IOException {
		Resource[] resources = loader.getResources(path);
		assertThat(resources, notNullValue());
		assertThat(resources.length, is(1));
		assertThat(resources[0].exists(), is(shouldExist));
		Resource resource = loader.getResource(path);
		assertThat(resource, notNullValue());
		assertThat(resource.exists(), is(shouldExist));
	}

	private static void assertFileViaLoaderWithPatter(HdfsResourceLoader loader, String path, boolean shouldExist) throws IOException {
		Resource[] resources = loader.getResources(path);
		assertThat(resources, notNullValue());
		assertThat(resources[0].exists(), is(shouldExist));
	}

}