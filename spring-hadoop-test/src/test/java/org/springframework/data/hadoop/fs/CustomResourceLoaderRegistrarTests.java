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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Tests for {@code CustomResourceLoaderRegistrar}.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CustomResourceLoaderRegistrarTests {

	@Autowired
	private ApplicationContext context;

	@Test
	public void testPathsForContextGetResource() {
		Resource resource1 = context.getResource("default2.txt");
		assertThat(resource1, notNullValue());
		assertThat(resource1, instanceOf(HdfsResource.class));

		Resource resource2 = context.getResource("hdfs:/hdfs.txt");
		assertThat(resource2, notNullValue());
		assertThat(resource2, instanceOf(HdfsResource.class));

		Resource resource3 = context.getResource("classpath:test.properties");
		assertThat(resource3, notNullValue());
		assertThat(resource3, instanceOf(ClassPathResource.class));

		Resource resource4 = context.getResource("file:/tmp/test.txt");
		assertThat(resource4, notNullValue());
		assertThat(resource4, instanceOf(UrlResource.class));

		Resource resource5 = context.getResource("http://example.com/tmp/test.txt");
		assertThat(resource5, notNullValue());
		assertThat(resource5, instanceOf(UrlResource.class));
	}

	@Test
	public void testPathsForContextGetResources() throws IOException {
		Resource[] resources1 = context.getResources("hdfs:default.txt");
		assertThat(resources1, notNullValue());
		assertThat(resources1.length, is(1));
		assertThat(resources1[0], instanceOf(HdfsResource.class));

		Resource[] resources2 = context.getResources("hdfs:/*");
		assertThat(resources2, notNullValue());

		Resource[] resources3 = context.getResources("hdfs://localhost:8020/*");
		assertThat(resources3, notNullValue());

		Resource[] resources4 = context.getResources("hdfs://localhost/*");
		assertThat(resources4, notNullValue());

		Resource[] resources5 = context.getResources("/*");
		assertThat(resources5, notNullValue());

		Resource[] resources6 = context.getResources("classpath:cfg*properties");
		assertThat(resources6, notNullValue());

	}

	@Test
	public void testNonExistPathsForContextGetResources() throws IOException {
		Resource[] resources1 = context.getResources("hdfs:/path/not/exist/*");
		assertThat(resources1, notNullValue());

		Resource[] resources2 = context.getResources("hdfs://localhost:8020/path/not/exist/*");
		assertThat(resources2, notNullValue());

		Resource[] resources3 = context.getResources("hdfs://localhost/path/not/exist/*");
		assertThat(resources3, notNullValue());

		Resource[] resources4 = context.getResources("/path/not/exist/*");
		assertThat(resources4, notNullValue());
	}
	
	@Test
	public void testPathsForBeanResourceEditor() {
		TestBean testBean1 = context.getBean("testBeanDefault", TestBean.class);
		assertThat(testBean1, notNullValue());
		assertThat(testBean1.getResource(), instanceOf(HdfsResource.class));

		TestBean testBean2 = context.getBean("testBeanHdfs", TestBean.class);
		assertThat(testBean2, notNullValue());
		assertThat(testBean2.getResource(), instanceOf(HdfsResource.class));

		TestBean testBean3 = context.getBean("testBeanClasspath", TestBean.class);
		assertThat(testBean3, notNullValue());
		assertThat(testBean3.getResource(), instanceOf(ClassPathResource.class));

		TestBean testBean4 = context.getBean("testBeanHttp", TestBean.class);
		assertThat(testBean4, notNullValue());
		assertThat(testBean4.getResource(), instanceOf(UrlResource.class));

		TestBean testBean5 = context.getBean("testBeanFile", TestBean.class);
		assertThat(testBean5, notNullValue());
		assertThat(testBean5.getResource(), instanceOf(UrlResource.class));

		TestBean testBean6 = context.getBean("testBeanHdfsResources1", TestBean.class);
		assertThat(testBean6, notNullValue());
	}

	@Test
	public void testNonExistPathsForBeanResourceEditor() {
		TestBean testBean1 = context.getBean("testBeanHdfsResources2", TestBean.class);
		assertThat(testBean1, notNullValue());
	}
	
	public static class TestBean {
		public Resource resource;
		public Resource[] resources;
		public TestBean(){}
		public void setResource(Resource resource) {
			this.resource = resource;
		}
		public Resource getResource() {
			return resource;
		}
		public void setResources(Resource[] resources) {
			this.resources = resources;
		}
		public Resource[] getResources() {
			return resources;
		}
	}

}
