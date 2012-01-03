/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.io;

import java.net.URI;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class DistributedCacheTest {

	@Autowired
	Configuration cfg;

	@Autowired
	ApplicationContext ctx;

	@Before
	public void init() throws Exception {
		TestUtils.writeToFS(cfg, "local/" + UUID.randomUUID() + ".txt");
		TestUtils.writeToFS(cfg, "local/" + UUID.randomUUID() + ".txt");
		TestUtils.writeToFS(cfg, "local/" + UUID.randomUUID() + ".txt");

		HdfsResourceLoader loader = new HdfsResourceLoader(cfg);
		assertTrue(loader.getResources("~/local/*.txt").length >= 3);
		ctx.getBean("hadoop-cache");
	}

	@After
	public void destroy() throws Exception {
		FileSystem fs = FileSystem.get(cfg);
		fs.delete(new Path("local/"), true);
	}

	@Test
	public void testClassPathArchives() throws Exception {
		Path[] archives = DistributedCache.getArchiveClassPaths(cfg);
		assertEquals(1, archives.length);
		assertEquals(new Path("/cp/some-zip.zip"), archives[0]);
	}

	@Test
	public void testClassPathFiles() throws Exception {
		Path[] files = DistributedCache.getFileClassPaths(cfg);
		assertEquals(1, files.length);
		assertEquals(new Path("/cp/some-library.jar"), files[0]);
	}

	@Test
	public void testCacheArchives() throws Exception {
		URI[] archives = DistributedCache.getCacheArchives(cfg);
		assertEquals(2, archives.length);
		assertEquals("/cp/some-zip.zip", archives[0].getPath());
		assertEquals("some-zip.zip", archives[0].getFragment());
		assertEquals("/cache/some-archive.tgz", archives[1].getPath());
		assertEquals("main-archive", archives[1].getFragment());
	}

	@Test
	public void testCacheFiles() throws Exception {
		URI[] files = DistributedCache.getCacheFiles(cfg);
		assertEquals(2, files.length);
		assertEquals("/cp/some-library.jar", files[0].getPath());
		assertEquals("library.jar", files[0].getFragment());
		assertEquals("/cache/some-resource.res", files[1].getPath());
		assertEquals("some-resource.res", files[1].getFragment());
	}

	@Test
	public void testLocalFiles() throws Exception {
		Path[] files = DistributedCache.getLocalCacheFiles(cfg);
		// include the 3 files from the pattern matcher
		assertEquals(4, files.length);
		assertEquals("some-file.txt", files[0].getName());
	}

	@Test
	public void testLocalArchives() throws Exception {
		Path[] archives = DistributedCache.getLocalCacheArchives(cfg);
		assertEquals(1, archives.length);
		assertEquals("some-tar.tar", archives[0].getName());
	}

	@Test
	public void testMisc() throws Exception {
		assertTrue(ctx.containsBean("hadoop-cache"));
		assertTrue(DistributedCache.getSymlink(cfg));
	}
}