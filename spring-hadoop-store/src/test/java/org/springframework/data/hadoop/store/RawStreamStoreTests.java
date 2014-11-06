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
package org.springframework.data.hadoop.store;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.data.hadoop.store.codec.Codecs;
import org.springframework.data.hadoop.store.input.TextFileReader;
import org.springframework.data.hadoop.store.output.OutputStreamWriter;
import org.springframework.data.hadoop.store.strategy.naming.ChainedFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.CodecFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.store.support.StoreUtils;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests for writing raw byte arrays.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class RawStreamStoreTests extends AbstractStoreTests {

	@org.springframework.context.annotation.Configuration
	static class Config {
		// just empty to survive without xml configs
	}

	@Test
	public void testWriteReadTextTwoLines() throws IOException {
		String[] dataArray = new String[] { DATA10, DATA11 };

		byte[][] data = new byte[4][];
		data[0] = DATA10.getBytes();
		data[1] = "\n".getBytes();
		data[2] = DATA11.getBytes();
		data[3] = "\n".getBytes();

		OutputStreamWriter writer = new OutputStreamWriter(getConfiguration(), testDefaultPath, null);
		TestUtils.writeData(writer, data, true);

		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath, null);
		TestUtils.readDataAndAssert(reader, dataArray);
	}

	@Test
	public void testStreamSmall() throws IOException {
		ByteArrayInputStream stream = new ByteArrayInputStream(DATA10.getBytes());
		OutputStreamWriter writer = new OutputStreamWriter(getConfiguration(), testDefaultPath, null);

		doWithInputStream(stream, writer);

		String[] dataArray = new String[] { DATA10 };
		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath, null);
		TestUtils.readDataAndAssert(reader, dataArray);
	}

	@Test
	public void testStreamBig() throws IOException {
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i<1000; i++) {
			buf.append(DATA10);
			buf.append("\n");
		}
		ByteArrayInputStream stream = new ByteArrayInputStream(buf.toString().getBytes());
		OutputStreamWriter writer = new OutputStreamWriter(getConfiguration(), testDefaultPath, null);

		doWithInputStream(stream, writer);

		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath, null);
		List<String> data = TestUtils.readData(reader);
		assertThat(data.size(), is(1000));
	}

	@Test
	public void testStreamBigWithCodec() throws IOException {
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i<1000; i++) {
			buf.append(DATA10);
			buf.append("\n");
		}
		ByteArrayInputStream stream = new ByteArrayInputStream(buf.toString().getBytes());
		OutputStreamWriter writer = new OutputStreamWriter(getConfiguration(), testDefaultPath, Codecs.GZIP.getCodecInfo());

		doWithInputStream(stream, writer);

		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath, Codecs.GZIP.getCodecInfo());
		List<String> data = TestUtils.readData(reader);
		assertThat(data.size(), is(1000));
	}

	@Test
	public void testStreamBigWithCodecWithStrategys() throws IOException {
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i<1000; i++) {
			buf.append(DATA10);
			buf.append("\n");
		}
		ByteArrayInputStream stream = new ByteArrayInputStream(buf.toString().getBytes());
		OutputStreamWriter writer = new OutputStreamWriter(getConfiguration(), testDefaultPath, Codecs.GZIP.getCodecInfo());

		ChainedFileNamingStrategy fileNamingStrategy = new ChainedFileNamingStrategy();
		fileNamingStrategy.register(new StaticFileNamingStrategy("data"));
		fileNamingStrategy.register(new CodecFileNamingStrategy());
		writer.setFileNamingStrategy(fileNamingStrategy);

		doWithInputStream(stream, writer);

		TextFileReader reader = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "data.gz"), Codecs.GZIP.getCodecInfo());
		List<String> data = TestUtils.readData(reader);
		assertThat(data.size(), is(1000));
	}

	/**
	 * Kinda simulating RemoteFileTemplate
	 */
	private static void doWithInputStream(InputStream stream, DataStoreWriter<byte[]> writer) throws IOException {
		// if used together with RemoteFileTemplate, DataStoreWriter could
		// be set via Tasklet constructor and marked as final to be
		// used within a InputStreamCallback
		StoreUtils.copy(stream, writer);
	}

}
