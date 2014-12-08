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
import static org.hamcrest.Matchers.instanceOf;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.data.hadoop.store.codec.Codecs;
import org.springframework.data.hadoop.store.input.TextFileReader;
import org.springframework.data.hadoop.store.output.TextFileWriter;
import org.springframework.data.hadoop.store.strategy.naming.ChainedFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.CodecFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.RollingFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.SizeRolloverStrategy;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.tests.Assume;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests for writing and reading text using text file.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class TextFileStoreTests extends AbstractStoreTests {

	@org.springframework.context.annotation.Configuration
	static class Config {
		// just empty to survive without xml configs
	}

	@Test
	public void testWriteReadTextOneLine() throws IOException {
		String[] dataArray = new String[] { DATA10 };

		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath, null);
		TestUtils.writeData(writer, dataArray);

		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath, null);
		TestUtils.readDataAndAssert(reader, dataArray);
	}

	@Test
	public void testWriteReadTextManyLines() throws IOException {
		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath, null);
		TestUtils.writeData(writer, DATA09ARRAY);

		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath, null);
		TestUtils.readDataAndAssert(reader, DATA09ARRAY);
	}

	@Test
	public void testWriteReadManyLinesWithGzip() throws IOException {
		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath,
				Codecs.GZIP.getCodecInfo());
		TestUtils.writeData(writer, DATA09ARRAY);

		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath,
				Codecs.GZIP.getCodecInfo());
		TestUtils.readDataAndAssert(reader, DATA09ARRAY);
	}

	@Test
	public void testWriteReadManyLinesWithBzip2() throws IOException {
		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath,
				Codecs.BZIP2.getCodecInfo());
		TestUtils.writeData(writer, DATA09ARRAY);

		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath,
				Codecs.BZIP2.getCodecInfo());
		TestUtils.readDataAndAssert(reader, DATA09ARRAY);
	}

	@Test
	public void testWriteReadManyLinesWithLzo() throws IOException {
		// Add lzo and native libs with project properties to run this test
		// because we can't include anything automatically
		// -PtestJavaLibraryPath=/tmp/lzo/native
		// -PtestJavaClasspath=/tmp/lzo/lib/hadoop-lzo-0.4.17.jar
		Assume.codecExists(Codecs.LZO.getCodecInfo().getCodecClass());
		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath,
				Codecs.LZO.getCodecInfo());
		TestUtils.writeData(writer, DATA09ARRAY);

		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath,
				Codecs.LZO.getCodecInfo());
		TestUtils.readDataAndAssert(reader, DATA09ARRAY);
	}

	@Test
	public void testWriteReadManyLinesWithGzipWithCodecNaming() throws IOException {
		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath,
				Codecs.GZIP.getCodecInfo());
		ChainedFileNamingStrategy fileNamingStrategy = new ChainedFileNamingStrategy();
		fileNamingStrategy.register(new CodecFileNamingStrategy());
		fileNamingStrategy.register(new StaticFileNamingStrategy());
		writer.setFileNamingStrategy(fileNamingStrategy);
		TestUtils.writeData(writer, DATA09ARRAY);

		TextFileReader reader = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "data.gz"),
				Codecs.GZIP.getCodecInfo());
		TestUtils.readDataAndAssert(reader, DATA09ARRAY);
	}

	@Test
	public void testWriteReadManyLinesWithNamingAndRollover() throws IOException {

		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath, null);
		writer.setFileNamingStrategy(new RollingFileNamingStrategy());
		writer.setRolloverStrategy(new SizeRolloverStrategy(40));
		writer.setIdleTimeout(10000);

		TestUtils.writeData(writer, DATA09ARRAY);

		TextFileReader reader1 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "0"), null);
		List<String> splitData1 = TestUtils.readData(reader1);

		TextFileReader reader2 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "1"), null);
		List<String> splitData2 = TestUtils.readData(reader2);

		TextFileReader reader3 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "2"), null);
		List<String> splitData3 = TestUtils.readData(reader3);

		assertThat(splitData1.size() + splitData2.size() + splitData3.size(), is(DATA09ARRAY.length));
	}

	@Test
	public void testWriteReadManyLinesWithNamingAndRolloverWithGzip() throws IOException {

		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath, Codecs.GZIP.getCodecInfo());
		writer.setFileNamingStrategy(new RollingFileNamingStrategy());
		writer.setRolloverStrategy(new SizeRolloverStrategy(40));

		// codec is buffering so we need to write some amount of
		// data before anything is actually written into a file/stream
		// writing same data over and over again is compressing a lot
		for (int i = 0; i<45000; i++) {
			TestUtils.writeData(writer, DATA09ARRAY, false);
		}
		TestUtils.writeData(writer, DATA09ARRAY, true);

		TextFileReader reader1 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "0"), Codecs.GZIP.getCodecInfo());
		List<String> splitData1 = TestUtils.readData(reader1);

		TextFileReader reader2 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "1"), Codecs.GZIP.getCodecInfo());
		List<String> splitData2 = TestUtils.readData(reader2);

		assertThat(splitData1.size() + splitData2.size(), is(450010));
	}

	@Test
	public void testContinueStrategies() throws IOException, InterruptedException {
		String[] dataArray = new String[] { DATA10 };

		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath, null);

		ChainedFileNamingStrategy fileNamingStrategy = new ChainedFileNamingStrategy();
		fileNamingStrategy.register(new StaticFileNamingStrategy("data"));
		fileNamingStrategy.register(new RollingFileNamingStrategy());
		writer.setFileNamingStrategy(fileNamingStrategy);
		writer.setInWritingSuffix(".tmp");
		writer.afterPropertiesSet();
		TestUtils.writeData(writer, dataArray);

		Thread.sleep(1000);

		writer = new TextFileWriter(getConfiguration(), testDefaultPath, null);
		fileNamingStrategy = new ChainedFileNamingStrategy();
		fileNamingStrategy.register(new StaticFileNamingStrategy("data"));
		fileNamingStrategy.register(new RollingFileNamingStrategy());
		writer.setFileNamingStrategy(fileNamingStrategy);
		writer.setInWritingSuffix(".tmp");
		writer.afterPropertiesSet();
		TestUtils.writeData(writer, dataArray);

		Thread.sleep(1000);

		writer = new TextFileWriter(getConfiguration(), testDefaultPath, null);
		fileNamingStrategy = new ChainedFileNamingStrategy();
		fileNamingStrategy.register(new StaticFileNamingStrategy("data"));
		fileNamingStrategy.register(new RollingFileNamingStrategy());
		writer.setFileNamingStrategy(fileNamingStrategy);
		writer.setInWritingSuffix(".tmp");
		writer.afterPropertiesSet();
		TestUtils.writeData(writer, dataArray);

		TextFileReader reader1 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "data-0"), null);
		List<String> splitData1 = TestUtils.readData(reader1);

		TextFileReader reader2 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "data-1"), null);
		List<String> splitData2 = TestUtils.readData(reader2);

		TextFileReader reader3 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "data-2"), null);
		List<String> splitData3 = TestUtils.readData(reader3);

		assertThat(splitData1.size() + splitData2.size() + splitData3.size(), is(3));
	}

	@Test
	public void testContinueStrategiesWithCodec() throws IOException, InterruptedException {
		String[] dataArray = new String[] { DATA10 };

		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath, Codecs.GZIP.getCodecInfo());

		ChainedFileNamingStrategy fileNamingStrategy = new ChainedFileNamingStrategy();
		fileNamingStrategy.register(new StaticFileNamingStrategy("data"));
		fileNamingStrategy.register(new RollingFileNamingStrategy());
		fileNamingStrategy.register(new CodecFileNamingStrategy());
		writer.setFileNamingStrategy(fileNamingStrategy);
		writer.setInWritingSuffix(".tmp");
		writer.afterPropertiesSet();
		TestUtils.writeData(writer, dataArray);

		Thread.sleep(1000);

		writer = new TextFileWriter(getConfiguration(), testDefaultPath, Codecs.GZIP.getCodecInfo());
		fileNamingStrategy = new ChainedFileNamingStrategy();
		fileNamingStrategy.register(new StaticFileNamingStrategy("data"));
		fileNamingStrategy.register(new RollingFileNamingStrategy());
		fileNamingStrategy.register(new CodecFileNamingStrategy());
		writer.setFileNamingStrategy(fileNamingStrategy);
		writer.setInWritingSuffix(".tmp");
		writer.afterPropertiesSet();
		TestUtils.writeData(writer, dataArray);

		Thread.sleep(1000);

		writer = new TextFileWriter(getConfiguration(), testDefaultPath, Codecs.GZIP.getCodecInfo());
		fileNamingStrategy = new ChainedFileNamingStrategy();
		fileNamingStrategy.register(new StaticFileNamingStrategy("data"));
		fileNamingStrategy.register(new RollingFileNamingStrategy());
		fileNamingStrategy.register(new CodecFileNamingStrategy());
		writer.setFileNamingStrategy(fileNamingStrategy);
		writer.setInWritingSuffix(".tmp");
		writer.afterPropertiesSet();
		TestUtils.writeData(writer, dataArray);

		TextFileReader reader1 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "data-0.gz"), Codecs.GZIP.getCodecInfo());
		List<String> splitData1 = TestUtils.readData(reader1);

		TextFileReader reader2 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "data-1.gz"), Codecs.GZIP.getCodecInfo());
		List<String> splitData2 = TestUtils.readData(reader2);

		TextFileReader reader3 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "data-2.gz"), Codecs.GZIP.getCodecInfo());
		List<String> splitData3 = TestUtils.readData(reader3);

		assertThat(splitData1.size() + splitData2.size() + splitData3.size(), is(3));
	}

	@Test
	public void testHdfsAvailableAfterWriterInits() throws Exception {
		Configuration failConfiguration = new Configuration();
		failConfiguration.set("fs.defaultFS", "hdfs://localhost:12345");
		String[] dataArray = new String[] { DATA10 };

		// use configuration which would not work for hdfs
		TextFileWriter writer = new TextFileWriter(failConfiguration, testDefaultPath, null);
		writer.afterPropertiesSet();
		writer.start();

		// restore configuration after writer lifecycle has been started
		TestUtils.setField("configuration", writer, getConfiguration());
		TestUtils.writeData(writer, dataArray);

		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath, null);
		TestUtils.readDataAndAssert(reader, dataArray);
	}

	@Test
	public void testHdfsAvailableAfterWriterInitsSeeWriteException() throws Exception {
		Configuration failConfiguration = new Configuration();
		failConfiguration.set("fs.defaultFS", "hdfs://localhost:12345");
		String[] dataArray = new String[] { DATA10 };

		// use configuration which would not work for hdfs
		TextFileWriter writer = new TextFileWriter(failConfiguration, testDefaultPath, null);
		writer.afterPropertiesSet();
		writer.start();

		// test write exception before we switch configuration
		Exception e = null;
		try {
			TestUtils.writeData(writer, dataArray, false);
		} catch (Exception ee) {
			e = ee;
		}
		assertThat(e, instanceOf(StoreException.class));

		// restore configuration after writer lifecycle has been started
		TestUtils.setField("configuration", writer, getConfiguration());
		TestUtils.writeData(writer, dataArray);

		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath, null);
		TestUtils.readDataAndAssert(reader, dataArray);
	}

}

