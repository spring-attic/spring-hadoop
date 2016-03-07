/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.yarn.fs;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.junit.Test;
import org.springframework.yarn.fs.LocalResourcesSelector.Entry;

/**
 * Tests for {@link AbstractLocalResourcesSelector} and {@link LocalResourcesSelector}.
 *
 * @author Janne Valkealahti
 *
 */
public class LocalResourcesSelectorTests {

	@Test
	public void testDefaults() {
		TestLocalResourcesSelector selector = new TestLocalResourcesSelector();
		List<Entry> entries = selector.select("");
		assertThat(entries, notNullValue());
		assertThat(entries.size(), is(2));

		Entry entry = findEntry("application.yml", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("application.properties", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());
	}

	@Test
	public void testDefaultsWithDir() {
		TestLocalResourcesSelector selector = new TestLocalResourcesSelector();
		List<Entry> entries = selector.select("/foo/");
		assertThat(entries, notNullValue());
		assertThat(entries.size(), is(2));

		Entry entry = findEntry("/foo/application.yml", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("/foo/application.properties", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

	}

	@Test
	public void testAddPatterns() {
		TestLocalResourcesSelector selector = new TestLocalResourcesSelector();
		selector.addPattern("foo.jar");
		selector.addPattern("foo.zip");
		List<Entry> entries = selector.select("");
		assertThat(entries, notNullValue());
		assertThat(entries.size(), is(4));

		Entry entry = findEntry("application.yml", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("application.properties", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("foo.jar", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("foo.zip", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), is(LocalResourceType.ARCHIVE));
	}

	@Test
	public void testAddPatternsWithDir() {
		TestLocalResourcesSelector selector = new TestLocalResourcesSelector();
		selector.addPattern("foo.jar");
		selector.addPattern("foo.zip");
		List<Entry> entries = selector.select("/foo/");
		assertThat(entries, notNullValue());
		assertThat(entries.size(), is(4));

		Entry entry = findEntry("/foo/application.yml", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("/foo/application.properties", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("/foo/foo.jar", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("/foo/foo.zip", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), is(LocalResourceType.ARCHIVE));
	}

	@Test
	public void testChangedPropertyFiles() {
		TestLocalResourcesSelector selector = new TestLocalResourcesSelector();
		selector.setPropertiesNames("props1", "props2");
		selector.setPropertiesSuffixes("suffix1", "suffix2");
		List<Entry> entries = selector.select("");
		assertThat(entries, notNullValue());
		assertThat(entries.size(), is(4));

		Entry entry = findEntry("props1.suffix1", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("props1.suffix2", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("props2.suffix1", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("props2.suffix2", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());
	}

	@Test
	public void testChangedNullZipPattern() {
		TestLocalResourcesSelector selector = new TestLocalResourcesSelector();
		selector.addPattern("foo.jar");
		selector.addPattern("foo.zip");
		selector.setZipArchivePattern(null);
		List<Entry> entries = selector.select("");
		assertThat(entries, notNullValue());
		assertThat(entries.size(), is(4));

		Entry entry = findEntry("foo.jar", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("foo.zip", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());
	}

	@Test
	public void testChangedZipPattern() {
		TestLocalResourcesSelector selector = new TestLocalResourcesSelector();
		selector.addPattern("foo1.zip");
		selector.addPattern("foo2.zip");
		selector.setZipArchivePattern("*1.zip");
		List<Entry> entries = selector.select("");
		assertThat(entries, notNullValue());
		assertThat(entries.size(), is(4));

		Entry entry = findEntry("foo1.zip", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), is(LocalResourceType.ARCHIVE));

		entry = findEntry("foo2.zip", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());
	}

	@Test
	public void testFoo() {
		TestLocalResourcesSelector selector = new TestLocalResourcesSelector();
		selector.addPattern("/path/from/root/file.txt");
		List<Entry> entries = selector.select("");
		assertThat(entries, notNullValue());
		assertThat(entries.size(), is(3));
		assertThat(entries.get(2).getPath(), is("/path/from/root/file.txt"));

		selector = new TestLocalResourcesSelector();
		selector.addPattern("/path/from/root/file.txt");
		entries = selector.select("/tmp");
		assertThat(entries, notNullValue());
		assertThat(entries.size(), is(3));
		assertThat(entries.get(2).getPath(), is("/path/from/root/file.txt"));
	}

	private static class TestLocalResourcesSelector extends AbstractLocalResourcesSelector {
	}

	private static Entry findEntry(String path, List<Entry> entries) {
		for (Entry e : entries) {
			if (e.getPath().equals(path)) {
				return e;
			}
		}
		return null;
	}

}
