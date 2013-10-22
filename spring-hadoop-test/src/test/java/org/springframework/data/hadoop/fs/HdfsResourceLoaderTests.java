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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

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

	@javax.annotation.Resource(name = "loaderHandleNoprefix")
	private HdfsResourceLoader loaderHandleNoprefix;

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

	@Test
	public void testFilesWithComplexPaths() throws Exception {
		String filePath1 = "/test/HdfsResourceLoaderTests/file1.txt";
		String filePath2 = "/test/HdfsResourceLoaderTests/dir1/file1.txt";
		String filePath3 = "/test/HdfsResourceLoaderTests/dir1/dir2/file1.txt";
		String filePath4 = "/test/HdfsResourceLoaderTests/file2.txt";
		String filePath5 = "/test/HdfsResourceLoaderTests/dir2/file2.txt";
		String filePath6 = "/test/HdfsResourceLoaderTests/dir2/dir2/file2.txt";

		TestUtils.writeToFS(loader, filePath1);
		TestUtils.writeToFS(loader, filePath2);
		TestUtils.writeToFS(loader, filePath3);
		TestUtils.writeToFS(loader, filePath4);
		TestUtils.writeToFS(loader, filePath5);
		TestUtils.writeToFS(loader, filePath6);

		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/file1*.txt", 1);
		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/*.txt", 2);

		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/*", 4);
		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/*txt", 2);
		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/**/*", 10);
		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/dir1/*", 2);
		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/dir1/file1.txt", 1);
		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/dir1/file?.txt", 1);
		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/dir1/?ile?.txt", 1);
		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/dir1/ile?.txt", 0);
		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/dir1/*.*", 1);
		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/dir1/foo*", 0);
		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/dir1/**/*", 3);

		assertFileCountViaLoaderWithPatter(loader, "/test/HdfsResourceLoaderTests/????/*", 4);
	}

	@Test
	public void testFilesNoprefix() throws Exception {
		String fileName1 = "HdfsResourceLoaderTests-testFilesNoprefix1.txt";
		Resource resource = loaderHandleNoprefix.getResource(fileName1);
		assertThat(resource, not(instanceOf(HdfsResource.class)));
		Resource[] resources = loaderHandleNoprefix.getResources(fileName1 + "*");
		assertThat(resources.length, is(0));
		resources = loaderHandleNoprefix.getResources("*");
		assertTrue(resources.length > 0);
		for (Resource r : resources) {
			assertThat(r, not(instanceOf(HdfsResource.class)));
		}
	}

	private static void assertFileCountViaLoaderWithPatter(HdfsResourceLoader loader, String path, int count) throws IOException {
		Resource[] resources = loader.getResources(path);
		assertThat(resources, notNullValue());
		assertThat(resources.length, is(count));
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
