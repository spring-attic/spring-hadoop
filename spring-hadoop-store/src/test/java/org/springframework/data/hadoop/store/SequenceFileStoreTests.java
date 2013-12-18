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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.data.hadoop.store.codec.Codecs;
import org.springframework.data.hadoop.store.input.TextSequenceFileReader;
import org.springframework.data.hadoop.store.output.TextSequenceFileWriter;
import org.springframework.data.hadoop.store.strategy.naming.RollingFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.SizeRolloverStrategy;

/**
 * Tests for writing and reading text using sequence file.
 *
 * @author Janne Valkealahti
 *
 */
public class SequenceFileStoreTests extends AbstractStoreTests {

	@Test
	public void testWriteReadSequenceFileOneLine() throws IOException {
		String[] dataArray = new String[] { DATA10 };

		TextSequenceFileWriter writer = new TextSequenceFileWriter(testConfig, testDefaultPath, null);
		TestUtils.writeData(writer, dataArray);

		TextSequenceFileReader reader = new TextSequenceFileReader(testConfig, testDefaultPath, null);
		TestUtils.readDataAndAssert(reader, dataArray);
	}

	@Test
	public void testWriteReadSequenceFileManyLines() throws IOException {
		TextSequenceFileWriter writer = new TextSequenceFileWriter(testConfig, testDefaultPath, null);
		TestUtils.writeData(writer, DATA09ARRAY);

		TextSequenceFileReader reader = new TextSequenceFileReader(testConfig, testDefaultPath, null);
		TestUtils.readDataAndAssert(reader, DATA09ARRAY);
	}

	// TODO: SequenceFile needs native stuff for codec
//	@Test
//	public void testWriteReadManyLinesWithGzip() throws IOException {
//		TextSequenceFileWriter writer = new TextSequenceFileWriter(testConfig, testDefaultPath,
//				Codecs.GZIP.getCodecInfo());
//		TestUtils.writeDataAndClose(writer, DATA09ARRAY);
//
//		TextSequenceFileReader reader = new TextSequenceFileReader(testConfig, testDefaultPath,
//				Codecs.GZIP.getCodecInfo());
//		TestUtils.readDataAndAssert(reader, DATA09ARRAY);
//	}

	@Test
	public void testWriteReadManyLinesWithBzip2() throws IOException {
		TextSequenceFileWriter writer = new TextSequenceFileWriter(testConfig, testDefaultPath,
				Codecs.BZIP2.getCodecInfo());
		TestUtils.writeData(writer, DATA09ARRAY);

		TextSequenceFileReader reader = new TextSequenceFileReader(testConfig, testDefaultPath,
				Codecs.BZIP2.getCodecInfo());
		TestUtils.readDataAndAssert(reader, DATA09ARRAY);
	}

	@Test
	public void testWriteReadManyLinesWithNamingAndRollover() throws IOException {
		TextSequenceFileWriter writer = new TextSequenceFileWriter(testConfig, testDefaultPath, null);
		writer.setFileNamingStrategy(new RollingFileNamingStrategy());
		writer.setRolloverStrategy(new SizeRolloverStrategy(150));
		writer.setIdleTimeout(10000);

		TestUtils.writeData(writer, DATA09ARRAY);

		TextSequenceFileReader reader1 = new TextSequenceFileReader(testConfig, new Path(testDefaultPath, "0"), null);
		List<String> splitData1 = TestUtils.readData(reader1);

		TextSequenceFileReader reader2 = new TextSequenceFileReader(testConfig, new Path(testDefaultPath, "1"), null);
		List<String> splitData2 = TestUtils.readData(reader2);

		TextSequenceFileReader reader3 = new TextSequenceFileReader(testConfig, new Path(testDefaultPath, "2"), null);
		List<String> splitData3 = TestUtils.readData(reader3);

		assertThat(splitData1.size() + splitData2.size() + splitData3.size(), is(DATA09ARRAY.length));
	}

}
