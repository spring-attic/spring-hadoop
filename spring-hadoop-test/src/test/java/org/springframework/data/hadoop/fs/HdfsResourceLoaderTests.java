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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
		String name1 = "HdfsResourceLoaderTests1.txt";
		String name2 = "HdfsResourceLoaderTests2.txt";

		TestUtils.writeToFS(loader, name1);
		TestUtils.writeToFS(loaderWithUser, name2);

		// getResources("~/" + name1)
		Resource[] resources1 = loader.getResources(name1);
		assertThat(resources1, notNullValue());
		assertThat(resources1.length, is(1));
		assertThat(resources1[0].exists(), is(true));

		Resource[] resources2 = loader.getResources(name2);
		assertThat(resources2, notNullValue());
		assertThat(resources2.length, is(1));
		assertThat(resources2[0].exists(), is(false));

		Resource[] resources3 = loaderWithUser.getResources(name1);
		assertThat(resources3, notNullValue());
		assertThat(resources3.length, is(1));
		assertThat(resources3[0].exists(), is(false));

		Resource[] resources4 = loaderWithUser.getResources(name2);
		assertThat(resources4, notNullValue());
		assertThat(resources4.length, is(1));
		assertThat(resources4[0].exists(), is(true));
	}

	@Test
	public void testFilesWithTilde() throws Exception {
		String name1 = "HdfsResourceLoaderTests3.txt";

		TestUtils.writeToFS(loader, name1);

//		Resource[] resources1 = loader.getResources("~/" + name1);
		Resource[] resources1 = loader.getResources("~/HdfsResourceLoaderTests3.txt*");
		assertThat(resources1, notNullValue());
		assertThat(resources1.length, is(1));
		assertThat(resources1[0].exists(), is(true));

	}

}
