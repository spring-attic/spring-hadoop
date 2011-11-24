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
package org.springframework.data.hadoop.io;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.data.hadoop.configuration.ConfigurationFactoryBean;

import static org.junit.Assert.*;

/**
 * Test for interacting with Hadoop HDFS.
 * 
 * @author Costin Leau
 */
public class HdfsResouceLoaderTest {

	private FileSystem fs;
	private HdfsResourceLoader loader;

	@Before
	public void before() throws Exception {
		Map<String, Object> props = new LinkedHashMap<String, Object>();
		props.put("fs.default.name", "hdfs://localhost");

		ConfigurationFactoryBean cfb = new ConfigurationFactoryBean();
		cfb.setBeanClassLoader(getClass().getClassLoader());
		cfb.setProperties(props);
		cfb.afterPropertiesSet();

		FileSystemFactoryBean fsf = new FileSystemFactoryBean();
		fsf.setConfiguration(cfb.getObject());
		fsf.afterPropertiesSet();

		fs = fsf.getObject();

		loader = new HdfsResourceLoader(fs);
	}

	@After
	public void after() throws Exception {
		fs.close();
	}

	@Test
	public void testPatternResolver() throws Exception {
		Resource[] resources = loader.getResources("**/*");
		System.out.println(resources.length);
		System.out.println(Arrays.toString(resources));

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

			assertTrue(resource instanceof WritableResource);
			WritableResource wr = (WritableResource) resource;
			assertTrue(wr.isWritable());

			byte[] bytes = name.getBytes();
			OutputStream out = wr.getOutputStream();

			out.write(bytes);
			out.close();

			assertTrue(resource.exists());
			assertTrue(resource.isReadable());
			assertTrue(resource.isOpen());

			InputStream in = resource.getInputStream();
			byte[] copy = new byte[bytes.length];
			in.read(copy);
			in.close();
			assertArrayEquals(bytes, copy);
		} finally {
			fs.delete(path, true);
		}
	}

	@Test
	public void testResolve() throws Exception {
		Resource resource = loader.getResource("/test");
		loader.setRegisterJvmUrl(true);

		System.out.println(((HdfsResource) resource).getPath().makeQualified(fs));

		System.out.println(resource.getURI());

		resource = loader.getResource("test");
		System.out.println(resource.getURI());
		System.out.println(resource.getURL());
	}
}