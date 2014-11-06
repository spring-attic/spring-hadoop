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
package org.springframework.data.hadoop.config;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Tests for 'hdp:configuration' tag.
 *
 * @author Costin Leau
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class ConfigurationNamespaceTests {

	@Resource(name = "hadoopConfiguration")
	private Configuration simple;

	@Resource
	private Configuration complex;

	@Resource
	private Configuration propsBased;

	@Resource
	private Configuration shortcutBased;

	@Test
	public void testSimpleConfiguration() throws Exception {
		assertThat(simple, notNullValue());
		assertThat(simple.get("test.name"), is("default"));
		assertThat(simple.get("test.name.2"), nullValue());
		assertThat(simple.get("resource.property"), nullValue());
		assertThat(simple.get("resource.property.2"), nullValue());
	}

	@Test
	public void testComplexConfiguration() throws Exception {
		assertThat(complex, notNullValue());
		assertThat(complex.get("test.name"), is("default"));
		assertThat(complex.get("test.name.2"), is("complex"));
		assertThat(complex.get("resource.property"), is("test-site.xml"));
		assertThat(complex.get("resource.property.2"), is("test-site-2.xml"));
	}

	@Test
	public void testPropertiesConfiguration() throws Exception {
		assertThat(propsBased, notNullValue());
		assertThat(propsBased.get("star"), is("chasing"));
		assertThat(propsBased.get("return"), is("captain eo"));
		assertThat(propsBased.get("train"), is("last"));
		assertThat(propsBased.get("dancing"), is("the dream"));
		assertThat(propsBased.get("tears"), is("in the mirror"));
		assertThat(propsBased.get("captain"), is("eo"));
	}

	@Test
	public void testShortcutConfiguration() throws Exception {
		assertThat(shortcutBased, notNullValue());
		assertThat(shortcutBased.get("fs.defaultFS"), is("hdfs://fakefs:1234"));
		assertThat(shortcutBased.get("yarn.resourcemanager.address"), is("fakerm:1234"));
		assertThat(shortcutBased.get("mapreduce.jobhistory.address"), is("fakejh:1234"));
	}

}
