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
 * Tests for {@link StaticBlockSplitter}.
 *
 * @author Janne Valkealahti
 *
 */
public class StaticBlockSplitterTests extends AbstractSplitterTests {

	@Test
	public void testOneBlockNoSplit() throws Exception {
		Path path = mockWithFileSystem(1,100);
		StaticBlockSplitter splitter = new StaticBlockSplitter(CONFIGURATION);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(1));
	}

	@Test
	public void testOneBlockOneSplit() throws Exception {
		Path path = mockWithFileSystem(1,100);
		StaticBlockSplitter splitter = new StaticBlockSplitter(CONFIGURATION, 1);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(2));
	}

	@Test
	public void testOneBlockTwoSplits() throws Exception {
		Path path = mockWithFileSystem(1,100);
		StaticBlockSplitter splitter = new StaticBlockSplitter(CONFIGURATION, 2);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(3));
	}

	@Test
	public void testTwoBlocksNoSplit() throws Exception {
		Path path = mockWithFileSystem(2,100);
		StaticBlockSplitter splitter = new StaticBlockSplitter(CONFIGURATION);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(2));
	}

	@Test
	public void testTwoBlocksTwoSplits() throws Exception {
		Path path = mockWithFileSystem(2,100);
		StaticBlockSplitter splitter = new StaticBlockSplitter(CONFIGURATION, 1);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(4));
	}

	@Test
	public void testTenBlocksThreeSplits() throws Exception {
		Path path = mockWithFileSystem(10,100);
		StaticBlockSplitter splitter = new StaticBlockSplitter(CONFIGURATION, 3);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(40));
	}

	@Test
	public void testWithFileSmallerThanBlock() throws Exception {
		Path path = mockWithFileSystem(0,100,50);
		StaticBlockSplitter splitter = new StaticBlockSplitter(CONFIGURATION);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(1));
	}

	@Test
	public void testSplitWithFileSmallerThanBlockSplit() throws Exception {
		Path path = mockWithFileSystem(0,100,40);
		StaticBlockSplitter splitter = new StaticBlockSplitter(CONFIGURATION, 1);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(1));
	}

	@Test
	public void testSplitWithFileBiggerThanBlockSplit() throws Exception {
		Path path = mockWithFileSystem(0,100,60);
		StaticBlockSplitter splitter = new StaticBlockSplitter(CONFIGURATION, 1);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits, notNullValue());
		assertThat(splits.size(), is(2));
	}

}
