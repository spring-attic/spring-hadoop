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
package org.springframework.yarn.boot.support;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.junit.Test;
import org.springframework.yarn.boot.support.BootLocalResourcesSelector.Mode;
import org.springframework.yarn.fs.LocalResourcesSelector.Entry;

/**
 * Tests for {@link BootLocalResourcesSelector}.
 *
 * @author Janne Valkealahti
 *
 */
public class BootLocalResourcesSelectorTests {

	@Test
	public void testAppmasterModeDefault() {
		BootLocalResourcesSelector selector = new BootLocalResourcesSelector(Mode.APPMASTER);
		List<Entry> entries = selector.select("");
		assertThat(entries, notNullValue());
		assertThat(entries.size(), is(4));

		Entry entry = findEntry("application.yml", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("application.properties", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("*appmaster*jar", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("*appmaster*zip", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), is(LocalResourceType.ARCHIVE));
	}

	@Test
	public void testContainerModeDefault() {
		BootLocalResourcesSelector selector = new BootLocalResourcesSelector(Mode.CONTAINER);
		List<Entry> entries = selector.select("");
		assertThat(entries, notNullValue());
		assertThat(entries.size(), is(4));

		Entry entry = findEntry("application.yml", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("application.properties", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("*container*jar", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), nullValue());

		entry = findEntry("*container*zip", entries);
		assertThat(entry, notNullValue());
		assertThat(entry.getType(), is(LocalResourceType.ARCHIVE));
	}

	@Test
	public void testNullModeDefault() {
		BootLocalResourcesSelector selector = new BootLocalResourcesSelector();
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

	private static Entry findEntry(String path, List<Entry> entries) {
		for (Entry e : entries) {
			if (e.getPath().equals(path)) {
				return e;
			}
		}
		return null;
	}

}
