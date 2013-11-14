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
package org.springframework.yarn.config;

import static org.junit.Assert.*;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Namespace tests for yarn:configuration elements.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration("/org/springframework/yarn/config/configuration-ns.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class ConfigurationNamespaceTest {

	@Resource(name = "yarnConfiguration")
	private Configuration defaultConfig;

	@Resource
	private Configuration complexConfig;

	@Resource
	private Configuration propsConfig;

	@Resource
	private Configuration shortcutConfig;

	@Test
	public void testDefaultConfiguration() throws Exception {
		assertNotNull(defaultConfig);
		assertEquals("jee", defaultConfig.get("test.foo"));
		assertNull(defaultConfig.get("test.foo.2"));
		assertNull(defaultConfig.get("resource.property"));
		assertNull(defaultConfig.get("resource.property.2"));
		assertEquals("10.10.10.10:8032", defaultConfig.get("yarn.resourcemanager.address"));
	}

	@Test
	public void testComplexConfiguration() throws Exception {
		assertNotNull(complexConfig);
		assertEquals("jee", complexConfig.get("test.foo"));
		assertEquals("10.10.10.10:8032", complexConfig.get("yarn.resourcemanager.address"));
		assertEquals("test-site-1.xml", complexConfig.get("resource.property"));
		assertEquals("test-site-2.xml", complexConfig.get("resource.property.2"));
	}

	@Test
	public void testPropertiesConfiguration() throws Exception {
		assertNotNull(propsConfig);
		assertEquals("jee20", propsConfig.get("foo20"));
		assertEquals("jee21", propsConfig.get("foo21"));
		assertEquals("jee22", propsConfig.get("foo22"));
		assertEquals("jee23", propsConfig.get("foo23"));
		assertEquals("jee24", propsConfig.get("foo24"));
		assertEquals("jee25", propsConfig.get("foo25"));
		assertEquals("jee26", propsConfig.get("foo26"));
		assertEquals("jee27", propsConfig.get("foo27"));
		assertEquals("jee28", propsConfig.get("foo28"));
	}

	@Test
	public void testShortcutConfiguration() throws Exception {
		assertNotNull(shortcutConfig);
		assertEquals("10.10.10.10:8032", shortcutConfig.get("yarn.resourcemanager.address"));
		assertEquals("10.10.10.10:8030", shortcutConfig.get("yarn.resourcemanager.scheduler.address"));
		assertEquals("hdfs://10.10.10.10:9000", shortcutConfig.get("fs.defaultFS"));
	}

}
