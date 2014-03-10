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
package org.springframework.data.hadoop.store.split;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Tests for {@link StaticLengthSplitter}.
 *
 * @author Janne Valkealahti
 *
 */
public class StaticLengthSplitterTests extends AbstractSplitterTests {

	@Test
	public void testSimpleSplit() throws Exception {
		Path path = mockWithFileSystem(1,100);
		StaticLengthSplitter splitter = new StaticLengthSplitter(CONFIGURATION, 20);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(5));
	}

	@Test
	public void testSplitLastNotFull() throws Exception {
		Path path = mockWithFileSystem(1,101);
		StaticLengthSplitter splitter = new StaticLengthSplitter(CONFIGURATION, 20);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(6));
	}

	@Test
	public void testSplitSpanMultipleBlocks() throws Exception {
		Path path = mockWithFileSystem(4,100);
		StaticLengthSplitter splitter = new StaticLengthSplitter(CONFIGURATION, 150);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(3));
	}

}
