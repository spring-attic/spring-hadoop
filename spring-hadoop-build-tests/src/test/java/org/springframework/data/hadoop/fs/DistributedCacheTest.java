/*
 * Copyright 2011-2013 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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
import org.springframework.data.hadoop.util.VersionUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class DistributedCacheTest {

	@Autowired
	Configuration cfg;
	@Autowired
	FileSystem fs;

	@Autowired
	ApplicationContext ctx;

	@Before
	public void init() throws Exception {
		TestUtils.writeToFS(cfg, "local/" + UUID.randomUUID() + ".txt");
		TestUtils.writeToFS(cfg, "local/" + UUID.randomUUID() + ".txt");
		TestUtils.writeToFS(cfg, "local/" + UUID.randomUUID() + ".txt");

		HdfsResourceLoader loader = new HdfsResourceLoader(cfg);
		assertTrue(loader.getResources("~/local/*.txt").length >= 3);
		loader.close();
		ctx.getBean("hadoopCache");
	}

	@After
	public void destroy() throws Exception {
		FileSystem fs = FileSystem.get(cfg);
		fs.delete(new Path("local/"), true);
	}

	// we do extra parsing since the classpath url behaves different on cloudera then Apache Vanilla
	@Test
	@SuppressWarnings("deprecation")
	public void testClassPathArchives() throws Exception {
		if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
			System.setProperty("path.separator", ":");
		}
		Path[] archives = DistributedCache.getArchiveClassPaths(cfg);
		assertEquals(2, archives.length);
		assertEquals(new Path("/cp/some-zip.zip"), archives[0]);
	}

	// we do extra parsing since the classpath url behaves different on cloudera  then Apache Vanilla
	@Test
	@SuppressWarnings("deprecation")
	public void testClassPathFiles() throws Exception {
		Path[] files = DistributedCache.getFileClassPaths(cfg);
		assertEquals(1, files.length);
		Path path = files[0];
		String p = path.toUri().getPath();
		// remove fragment
		int index = p.indexOf("#");
		if (index >= 0) {
			p = p.substring(0, index);
		}
		assertEquals("/cp/some-library.jar", p);
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testCacheArchives() throws Exception {
		URI[] archives = DistributedCache.getCacheArchives(cfg);
		System.out.println(Arrays.toString(archives));
		assertEquals(3, archives.length);
		assertEquals("/cp/some-zip.zip", archives[0].getPath());
		//assertEquals("some-zip.zip", archives[0].getFragment());
		assertEquals("/cp/some-extra-zip.zip", archives[1].getPath());
		assertEquals("/cache/some-archive.tgz", archives[2].getPath());
		//assertEquals("main-archive", archives[1].getFragment());
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testCacheFiles() throws Exception {
		URI[] files = DistributedCache.getCacheFiles(cfg);
		System.out.println(Arrays.toString(files));
		assertEquals(2, files.length);
		assertEquals("/cp/some-library.jar", files[0].getPath());
		assertEquals("library.jar", files[0].getFragment());
		assertEquals("/cache/some-resource.res", files[1].getPath());
		assertEquals("some-resource.res", files[1].getFragment());
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testLocalFiles() throws Exception {
		assumeTrue(!VersionUtils.isHadoop2X()); // TODO: need to figure out a way to support this
		Path[] files = DistributedCache.getLocalCacheFiles(cfg);
		// include the 3 files from the pattern matcher
		System.out.println(Arrays.toString(files));
		assertEquals(4, files.length);
		assertEquals("some-file.txt", files[0].getName());
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testLocalArchives() throws Exception {
		assumeTrue(!VersionUtils.isHadoop2X()); // TODO: need to figure out a way to support this
		Path[] archives = DistributedCache.getLocalCacheArchives(cfg);
		System.out.println(Arrays.toString(archives));
		assertEquals(1, archives.length);
		assertEquals("some-tar.tar", archives[0].getName());
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testMisc() throws Exception {
		assertTrue(ctx.containsBean("hadoopCache"));
		assertTrue(DistributedCache.getSymlink(cfg));
	}

	@Test
	public void testClassPathFragment() throws Exception {
		DistributedCacheFactoryBean dcache = new DistributedCacheFactoryBean();
		dcache.setConfiguration(cfg);
		dcache.setCreateSymlink(false);
		Set<DistributedCacheFactoryBean.CacheEntry> jars = new HashSet<DistributedCacheFactoryBean.CacheEntry>();
		jars.add(new DistributedCacheFactoryBean.CacheEntry(DistributedCacheFactoryBean.CacheEntry.EntryType.CP,
				"/cp/some-library-1.jar"));

		dcache.setEntries(jars);
		dcache.afterPropertiesSet();
	}
}