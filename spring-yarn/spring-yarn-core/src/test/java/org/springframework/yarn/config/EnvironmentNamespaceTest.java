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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Map;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.configuration.EnvironmentFactoryBean;

/**
 * Namespace tests for yarn:configuration elements.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration("/org/springframework/yarn/config/environment-ns.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class EnvironmentNamespaceTest {

	@Resource(name = "yarnEnvironment")
	private Map<String, String> defaultEnvironment;

	@Resource(name = "propsEnv")
	private Map<String, String> propsEnvironment;

	@Resource(name = "defClasspathEnv")
	private Map<String, String> defClasspathEnvironment;

	@Resource(name = "&defClasspathEnv")
	private EnvironmentFactoryBean environmentFactoryBeanDefClasspathEnv;

	@Resource(name = "defClasspathEnvMixed")
	private Map<String, String> defClasspathEnvironmentMixed;

	@Resource(name = "&defClasspathEnvMixed")
	private EnvironmentFactoryBean environmentFactoryBeanDefClasspathEnvMixed;

	@Test
	public void testDefaultEnvironment() throws Exception {
		assertNotNull(defaultEnvironment);
		assertEquals("jee", defaultEnvironment.get("foo"));
		assertNull(defaultEnvironment.get("test.foo.2"));
		assertEquals("myvalue1", defaultEnvironment.get("test-myvar1"));
	}

	@Test
	public void testPropertiesEnvironment() throws Exception {
		assertNotNull(propsEnvironment);
		assertEquals("jee20", propsEnvironment.get("foo20"));
		assertEquals("jee21", propsEnvironment.get("foo21"));
		assertEquals("jee22", propsEnvironment.get("foo22"));
		assertEquals("jee23", propsEnvironment.get("foo23"));
		assertEquals("jee24", propsEnvironment.get("foo24"));
		assertEquals("jee25", propsEnvironment.get("foo25"));
		assertEquals("jee26", propsEnvironment.get("foo26"));
		assertEquals("jee27", propsEnvironment.get("foo27"));
		assertEquals("jee28", propsEnvironment.get("foo28"));
	}

	@Test
	public void testEnvironmentWithClasspath() throws Exception {
		assertNotNull(defClasspathEnvironment);

		String classpath = defClasspathEnvironment.get("CLASSPATH");
		assertNotNull(classpath);

		// check that there's no extra or empty elements
		assertThat(false, is(classpath.contains("::")));
		assertThat(true, is(classpath.charAt(0) != ':'));
		assertThat(true, is(classpath.charAt(classpath.length()-1) != ':'));
	}

	@Test
	public void testEnvironmentWithMixedClasspath() throws Exception {
		assertNotNull(defClasspathEnvironmentMixed);
		String classpath = defClasspathEnvironmentMixed.get("CLASSPATH");
		assertNotNull(classpath);
		assertEquals("myvalue1", defClasspathEnvironmentMixed.get("test-myvar1"));
		assertEquals("jee", defClasspathEnvironmentMixed.get("foo"));
	}

}
