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
 * Tests for {@link SlopBlockSplitter}.
 *
 * @author Janne Valkealahti
 *
 */
public class SlopBlockSplitterTests extends AbstractSplitterTests {

	@Test
	public void testOneBlockNoSplit() throws Exception {
		Path path = mockWithFileSystem(1,100);
		SlopBlockSplitter splitter = new SlopBlockSplitter(CONFIGURATION);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(1));
	}

	@Test
	public void testSlopOverflows() throws Exception {
		Path path = mockWithFileSystem(1,100,10);
		SlopBlockSplitter splitter = new SlopBlockSplitter(CONFIGURATION);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(1));
	}

	@Test
	public void testSlopRemainingBlockTooLarge() throws Exception {
		Path path = mockWithFileSystem(1,100,11);
		SlopBlockSplitter splitter = new SlopBlockSplitter(CONFIGURATION);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(2));
	}

	@Test
	public void testTwoBlocksSplitByBlock() throws Exception {
		Path path = mockWithFileSystem(2,100);
		SlopBlockSplitter splitter = new SlopBlockSplitter(CONFIGURATION);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(2));
	}

	@Test
	public void testTwoBlocksSlopOverflows() throws Exception {
		Path path = mockWithFileSystem(2,100,10);
		SlopBlockSplitter splitter = new SlopBlockSplitter(CONFIGURATION);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(2));
	}

	@Test
	public void testTwoBlocksSlopRemainingBlockTooLarge() throws Exception {
		Path path = mockWithFileSystem(2,100,50);
		SlopBlockSplitter splitter = new SlopBlockSplitter(CONFIGURATION);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(3));
	}

}
