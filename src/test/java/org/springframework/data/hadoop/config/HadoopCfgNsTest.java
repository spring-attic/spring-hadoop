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
package org.springframework.data.hadoop.config;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
@ContextConfiguration("/org/springframework/data/hadoop/config/hadoop-ns.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class HadoopCfgNsTest {

	@Resource(name = "hadoop-config")
	private Configuration simple;

	@Resource
	private Configuration complex;

	@Test
	public void testSimpleConfiguration() throws Exception {
		assertNotNull(simple);
		assertEquals("default", simple.get("test.name"));
		assertNull(simple.get("test.name.2"));
		assertNull(simple.get("resource.property"));
		assertNull(simple.get("resource.property.2"));
	}

	@Test
	public void testComplexConfiguration() throws Exception {
		assertNotNull(complex);
		assertEquals("default", complex.get("test.name"));
		assertEquals("complex", complex.get("test.name.2"));
		assertEquals("test-site.xml", complex.get("resource.property"));
		assertEquals("test-site-2.xml", complex.get("resource.property.2"));
	}
}
