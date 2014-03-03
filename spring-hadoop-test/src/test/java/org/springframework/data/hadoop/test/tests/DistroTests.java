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
package org.springframework.data.hadoop.test.tests;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.springframework.util.ClassUtils;

/**
 * Tests for {@link Distro}.
 *
 * @author Janne Valkealahti
 */
public class DistroTests {

	@Test
	public void testOneShouldExist() {
		assertThat(Distro.resolveDistros().size(), not(0));
	}

	@Test
	public void testShouldSkipYarn() throws ClassNotFoundException, LinkageError {
		Assume.hadoopVersion(Version.HADOOP2X);
		Class<?> clazz = ClassUtils.forName("org.apache.hadoop.yarn.conf.YarnConfiguration", getClass().getClassLoader());
		assertThat(clazz, notNullValue());
	}

	@Test
	public void testHaveVersion() {
		assertThat(Version.resolveVersion(), notNullValue());
	}

}
