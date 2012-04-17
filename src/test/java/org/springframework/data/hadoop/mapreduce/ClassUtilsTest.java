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
package org.springframework.data.hadoop.mapreduce;

import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

import test.SomeClass;
import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
public class ClassUtilsTest {

	@Test
	public void testParentLastClassLoading() throws Exception {
		Resource jar = new DefaultResourceLoader().getResource("class-v1.jar");
		SomeClass v2 = new SomeClass();
		assertEquals("Class-v2", System.getProperty(SomeClass.LAST_LOADED));
		assertNotNull(System.getProperty(SomeClass.CLASS_LOADED + ".2"));

		// use v2 CL as a parent
		Object obj = ClassUtils.loadClassParentLast(jar, v2.getClass().getClassLoader(), "test.SomeClass", null);

		// check the jar classes are preferred
		assertEquals("Class-v1", System.getProperty(SomeClass.LAST_LOADED));
		assertNotNull(System.getProperty(SomeClass.CLASS_LOADED + ".1"));
		assertFalse(v2.getClass().equals(obj.getClass()));
	}

	// disabled test
	public void testNestedClass() throws Exception {
		Resource jar = new DefaultResourceLoader().getResource("jar-with-classes.jar");

		Object obj = ClassUtils.loadClassParentLast(jar, getClass().getClassLoader(), "test.SomeNestedClass", null);

		// check the jar classes are preferred
		assertEquals("NestedClass", System.getProperty(SomeClass.LAST_LOADED));
		assertNotNull(System.getProperty(SomeClass.CLASS_LOADED + ".nested"));
		assertFalse(org.springframework.util.ClassUtils.isPresent(obj.getClass().getName(), getClass().getClassLoader()));
	}

	@Test
	public void testNestedLib() throws Exception {
		Resource jar = new DefaultResourceLoader().getResource("jar-with-libs.jar");

		Object obj = ClassUtils.loadClassParentLast(jar, getClass().getClassLoader(), "test.SomeLibClass", null);

		// check the jar classes are preferred
		assertEquals("LibClass", System.getProperty(SomeClass.LAST_LOADED));
		assertNotNull(System.getProperty(SomeClass.CLASS_LOADED + ".lib"));
		assertFalse(org.springframework.util.ClassUtils.isPresent(obj.getClass().getName(), getClass().getClassLoader()));
	}
}
