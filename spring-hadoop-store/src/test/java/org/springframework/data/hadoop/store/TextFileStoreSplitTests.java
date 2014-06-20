/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.springframework.data.hadoop.store.codec.Codecs;
import org.springframework.data.hadoop.store.input.TextFileReader;
import org.springframework.data.hadoop.store.output.TextFileWriter;
import org.springframework.data.hadoop.store.split.Split;
import org.springframework.data.hadoop.store.split.Splitter;
import org.springframework.data.hadoop.store.split.StaticLengthSplitter;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests for writing and reading text using text file.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class TextFileStoreSplitTests extends AbstractStoreTests {

	@org.springframework.context.annotation.Configuration
	static class Config {
		// just empty to survive without xml configs
	}

	@Test
	public void testWriteReadSplitTextManyLines() throws IOException {
		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath, null);
		TestUtils.writeData(writer, DATA09ARRAY, false);
		TestUtils.writeData(writer, DATA09ARRAY, false);
		TestUtils.writeData(writer, DATA09ARRAY, true);

		Splitter splitter = new StaticLengthSplitter(getConfiguration(), 110l);
		List<Split> inputSplits = splitter.getSplits(testDefaultPath);
		assertNotNull(inputSplits);
		assertThat(inputSplits.size(), is(3));

		TextFileReader reader1 = new TextFileReader(getConfiguration(), testDefaultPath, null, inputSplits.get(0), null);
		List<String> readData1 = TestUtils.readData(reader1);
		assertNotNull(readData1);

		TextFileReader reader2 = new TextFileReader(getConfiguration(), testDefaultPath, null, inputSplits.get(1), null);
		List<String> readData2 = TestUtils.readData(reader2);
		assertNotNull(readData2);

		TextFileReader reader3 = new TextFileReader(getConfiguration(), testDefaultPath, null, inputSplits.get(2), null);
		List<String> readData3 = TestUtils.readData(reader3);
		assertNotNull(readData3);

		assertThat(readData1.size()+readData2.size()+readData3.size(), is(30));
	}

	@Test
	public void testWriteReadBZip2WithSplitSmallData() throws IOException {
		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath, Codecs.BZIP2.getCodecInfo());
		TestUtils.writeData(writer, DATA09ARRAY, false);
		TestUtils.writeData(writer, DATA09ARRAY, false);
		TestUtils.writeData(writer, DATA09ARRAY, true);

		Splitter splitter = new StaticLengthSplitter(getConfiguration(), 30l);
		List<Split> inputSplits = splitter.getSplits(testDefaultPath);
		assertNotNull(inputSplits);
		assertThat(inputSplits.size(), is(3));

		TextFileReader reader1 = new TextFileReader(getConfiguration(), testDefaultPath, Codecs.BZIP2.getCodecInfo(), inputSplits.get(0), null);
		List<String> readData1 = TestUtils.readData(reader1);
		assertNotNull(readData1);

		TextFileReader reader2 = new TextFileReader(getConfiguration(), testDefaultPath, Codecs.BZIP2.getCodecInfo(), inputSplits.get(1), null);
		List<String> readData2 = TestUtils.readData(reader2);
		assertNotNull(readData2);

		TextFileReader reader3 = new TextFileReader(getConfiguration(), testDefaultPath, Codecs.BZIP2.getCodecInfo(), inputSplits.get(2), null);
		List<String> readData3 = TestUtils.readData(reader3);
		assertNotNull(readData3);

		assertThat(readData1.size()+readData2.size()+readData3.size(), is(30));
	}

}
