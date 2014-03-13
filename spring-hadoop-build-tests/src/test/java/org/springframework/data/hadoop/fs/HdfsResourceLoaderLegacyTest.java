/*
 * Copyright 2011 the original author or authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.data.hadoop.configuration.ConfigurationFactoryBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test for interacting with Hadoop HDFS.
 *
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/org/springframework/data/hadoop/hadoop-ctx.xml")
public class HdfsResourceLoaderLegacyTest {

	@Autowired
	private Configuration cfg;
	private FileSystem fs;
	private HdfsResourceLoader loader;

	@Before
	public void before() throws Exception {
		FileSystemFactoryBean fsf = new FileSystemFactoryBean();
		fsf.setConfiguration(cfg);
		fsf.afterPropertiesSet();

		fs = fsf.getObject();

		System.out.println("Current user is " + UserGroupInformation.getCurrentUser());
		System.out.println("Home dir is " + fs.getHomeDirectory().toString());

		loader = new HdfsResourceLoader(cfg, null);
	}

	@After
	public void after() throws Exception {
		if (fs != null) {
			fs.close();
		}
	}

	@Test
	public void testPatternResolver() throws Exception {
		Resource[] resources = loader.getResources("**/*");
		System.out.println(resources.length);
		System.out.println(Arrays.toString(resources));

		Resource[] homeResources = loader.getResources("~/**/*");
		System.out.println(homeResources.length);
		System.out.println(Arrays.toString(resources));
		assertArrayEquals(resources, homeResources);

		Resource[] res = loader.getResources("/**/user/**/*");
		System.out.println(Arrays.toString(res));

		System.out.println(Arrays.toString(loader.getResources("/**/user/**/*.xml")));
	}

	@Test
	public void testConnectExistingFS() throws Exception {
		System.out.println(fs.toString());
		System.out.println(fs.getUri());
		System.out.println(fs.getHomeDirectory());
		FileStatus[] listStatus = fs.listStatus(new Path("/"));

		for (FileStatus fileStatus : listStatus) {
			System.out.println(fileStatus.getPath());
		}

		Resource resource = loader.getResource("/");
		System.out.println(resource.isReadable());
		System.out.println(resource.contentLength());
	}

	@Test
	public void testWriteable() throws Exception {
		String name = "test-" + UUID.randomUUID() + ".file";
		Path path = new Path(name);

		try {

			Resource resource = loader.getResource(name);
			System.out.println(resource.toString());
			assertFalse(resource.exists());
			assertFalse(resource.isReadable());
			assertFalse(resource.isOpen());

			resource = TestUtils.writeToFS(loader, name);
			assertTrue(resource.exists());
			assertTrue(resource.isReadable());
			assertTrue(resource.isOpen());

			InputStream in = resource.getInputStream();

			byte[] copy = new byte[name.length()];
			in.read(copy);
			in.close();
			assertArrayEquals(name.getBytes(), copy);
		} finally {
			fs.delete(path, true);
		}
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testResolve() throws Exception {
		Resource resource = loader.getResource("/test");
		try {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(fs.getConf()));
		} catch (Error err) {
			//somebody already registered an URL handler...
			// get out
		}

		System.out.println(((HdfsResource) resource).getPath().makeQualified(fs));
		System.out.println(resource.getURI());

		resource = loader.getResource("test");
		System.out.println(resource.getURI());
		System.out.println(resource.getURL());
	}

	@Test
	public void testPathWithFragment() throws Exception {
		String name = "test-" + UUID.randomUUID() + ".file#fragment";

		Path path = new Path(name);
		Resource resource = loader.getResource(name);

		try {
			System.out.println(resource.toString());
			assertFalse(resource.exists());
			assertFalse(resource.isReadable());
			assertFalse(resource.isOpen());

			resource = TestUtils.writeToFS(loader, resource.getURI().toString());
			//			assertTrue(resource instanceof WritableResource);
			//			WritableResource wr = (WritableResource) resource;
			//			assertTrue(wr.isWritable());
			//
			//			byte[] bytes = name.getBytes();
			//			OutputStream out = wr.getOutputStream();
			//
			//			out.write(bytes);
			//			out.close();

			assertTrue(resource.exists());
			assertTrue(resource.isReadable());
			assertTrue(resource.isOpen());

			URI uri = resource.getURI();
			assertEquals("fragment", uri.getFragment());

		} finally {
			fs.delete(path, true);
		}

	}

	@Test
	public void testFilesMatch() throws Exception {
		try {
			TestUtils.writeToFS(loader, "local/" + UUID.randomUUID() + ".txt");
			TestUtils.writeToFS(loader, "local/" + UUID.randomUUID() + ".txt");
			TestUtils.writeToFS(loader, "local/" + UUID.randomUUID() + ".txt");

			Resource[] resources = loader.getResources("local/*.txt");
			assertTrue(resources.length >= 3);
		} finally {
			fs.delete(new Path("local/"), true);
		}
	}

	@Test
	public void testFilesMatchWithHomePrefix() throws Exception {
		try {
			TestUtils.writeToFS(loader, "local/" + UUID.randomUUID() + ".txt");
			TestUtils.writeToFS(loader, "local/" + UUID.randomUUID() + ".txt");
			TestUtils.writeToFS(loader, "local/" + UUID.randomUUID() + ".txt");

			Resource[] resources = loader.getResources("~/local/*.txt");
			assertTrue(resources.length >= 3);
		} finally {
			fs.delete(new Path("local/"), true);
		}
	}

	@Test
	public void testFilesMatchWithPrefix() throws Exception {
		try {
			TestUtils.writeToFS(loader, "local/" + UUID.randomUUID() + ".txt");
			TestUtils.writeToFS(loader, "local/" + UUID.randomUUID() + ".txt");
			TestUtils.writeToFS(loader, "local/" + UUID.randomUUID() + ".txt");

			Resource[] resources = loader.getResources("./local/../local/*.txt");
			assertTrue(resources.length >= 3);
		} finally {
			fs.delete(new Path("local/"), true);
		}
	}

	@Test
	public void testDecompressedStream() throws Exception {
		DefaultCodec codec = new DefaultCodec();
		codec.setConf(fs.getConf());
		String name = "local/" + UUID.randomUUID() + codec.getDefaultExtension();
		OutputStream outputStream = codec.createOutputStream(fs.create(new Path(name)));
		byte[] content = name.getBytes();
		outputStream.write(content);
		outputStream.close();

		Resource resource = loader.getResource(name);
		assertNotNull(resource);
		InputStream inputStream = resource.getInputStream();
		assertEquals(DecompressorStream.class, inputStream.getClass());
		assertTrue(TestUtils.compareStreams(new ByteArrayInputStream(content), inputStream));
	}

	@Test
	public void testCompressedStream() throws Exception {

		DefaultCodec codec = new DefaultCodec();
		codec.setConf(fs.getConf());
		String name = "local/" + UUID.randomUUID() + codec.getDefaultExtension();
		OutputStream outputStream = codec.createOutputStream(fs.create(new Path(name)));
		byte[] content = name.getBytes();
		outputStream.write(content);
		outputStream.close();

		loader.setUseCodecs(false);

		try {
			Resource resource = loader.getResource(name);
			assertNotNull(resource);
			InputStream inputStream = resource.getInputStream();
			System.out.println(inputStream.getClass());
			assertFalse(DecompressorStream.class.equals(inputStream.getClass()));
			assertFalse(TestUtils.compareStreams(new ByteArrayInputStream(content), inputStream));
		} finally {
			loader.setUseCodecs(true);
		}
	}

	@Test
	public void testURLCycle() throws Exception {
		ConfigurationFactoryBean cfactory;
		cfactory = new ConfigurationFactoryBean();
		cfactory.setRegisterUrlHandler(true);
		cfactory.afterPropertiesSet();

		/* create hdfs resource loader */
		HdfsResourceLoader ldr = new HdfsResourceLoader(cfactory.getObject(), null, null);
		assertNotNull(ldr);
	}
}