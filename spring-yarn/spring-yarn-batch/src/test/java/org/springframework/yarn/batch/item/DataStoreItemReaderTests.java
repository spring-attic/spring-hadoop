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
package org.springframework.yarn.batch.item;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.store.DataStoreReader;
import org.springframework.data.hadoop.store.input.TextFileReader;
import org.springframework.data.hadoop.store.split.Split;
import org.springframework.data.hadoop.store.split.StaticLengthSplitter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.test.context.MiniYarnCluster;
import org.springframework.yarn.test.context.YarnDelegatingSmartContextLoader;
import org.springframework.yarn.test.junit.AbstractYarnClusterTests;

/**
 * Tests for {@link DataStoreItemReader}.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader = YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster
public class DataStoreItemReaderTests extends AbstractYarnClusterTests {

	@Test
	public void testWithOneReader() throws Exception {
		Path path = new Path("/syarn-tmp/DataStoreItemReaderTests-testWithOneReader/data.txt");
		createTestData(path);
		DataStoreReader<String> reader = new TextFileReader(configuration, path, null, null, null);
		DataStoreItemReader<String> itemReader = new DataStoreItemReader<String>();
		itemReader.setDataStoreReader(reader);
		itemReader.setLineDataMapper(new PassThroughLineDataMapper());
		int count = 0;
		while (itemReader.read() != null) {
			count++;
		}
		assertThat(count, is(300));
	}

	@Test
	public void testWithSplit() throws Exception {
		Path path = new Path("/syarn-tmp/DataStoreItemReaderTests-testWithSplit/data.txt");
		createTestData(path);

		StaticLengthSplitter splitter = new StaticLengthSplitter(getConfiguration(), 1500);
		List<Split> splits = splitter.getSplits(path);
		assertThat(splits.size(), is(2));

		DataStoreReader<String> reader = new TextFileReader(configuration, path, null, splits.get(0), null);
		DataStoreItemReader<String> itemReader = new DataStoreItemReader<String>();
		itemReader.setDataStoreReader(reader);
		itemReader.setLineDataMapper(new PassThroughLineDataMapper());
		int count = 0;
		while (itemReader.read() != null) {
			count++;
		}

		reader = new TextFileReader(configuration, path, null, splits.get(1), null);
		itemReader = new DataStoreItemReader<String>();
		itemReader.setDataStoreReader(reader);
		itemReader.setLineDataMapper(new PassThroughLineDataMapper());
		while (itemReader.read() != null) {
			count++;
		}

		assertThat(count, is(300));
	}

	@Test
	public void testRestore() throws Exception {
		Path path = new Path("/syarn-tmp/DataStoreItemReaderTests-testRestore/data.txt");
		createTestData(path);
		DataStoreReader<String> reader = new TextFileReader(configuration, path, null, null, null);
		DataStoreItemReader<String> itemReader = new DataStoreItemReader<String>();
		itemReader.setDataStoreReader(reader);
		itemReader.setLineDataMapper(new PassThroughLineDataMapper());

		ExecutionContext context = new ExecutionContext();
		context.putLong(DataStoreItemReader.READ_POSITION, 150);
		itemReader.open(context);

		int count = 0;
		while (itemReader.read() != null) {
			count++;
		}
		assertThat(count, is(150));
	}

	@Test
	public void testRestoreToEnd() throws Exception {
		Path path = new Path("/syarn-tmp/DataStoreItemReaderTests-testRestoreToEnd/data.txt");
		createTestData(path);
		DataStoreReader<String> reader = new TextFileReader(configuration, path, null, null, null);
		DataStoreItemReader<String> itemReader = new DataStoreItemReader<String>();
		itemReader.setDataStoreReader(reader);
		itemReader.setLineDataMapper(new PassThroughLineDataMapper());

		ExecutionContext context = new ExecutionContext();
		context.putLong(DataStoreItemReader.READ_POSITION, 300);
		itemReader.open(context);

		int count = 0;
		while (itemReader.read() != null) {
			count++;
		}
		assertThat(count, is(0));
	}

	@Test(expected = ItemStreamException.class)
	public void testRestoreFailure() throws Exception {
		Path path = new Path("/syarn-tmp/DataStoreItemReaderTests-testRestoreFailure/data.txt");
		createTestData(path);
		DataStoreReader<String> reader = new TextFileReader(configuration, path, null, null, null);
		DataStoreItemReader<String> itemReader = new DataStoreItemReader<String>();
		itemReader.setDataStoreReader(reader);
		itemReader.setLineDataMapper(new PassThroughLineDataMapper());

		ExecutionContext context = new ExecutionContext();
		context.putLong(DataStoreItemReader.READ_POSITION, 301);
		itemReader.open(context);
	}

	@Test
	public void testSavePosition() throws Exception {
		Path path = new Path("/syarn-tmp/DataStoreItemReaderTests-testSavePosition/data.txt");
		createTestData(path);
		DataStoreReader<String> reader = new TextFileReader(configuration, path, null, null, null);
		DataStoreItemReader<String> itemReader = new DataStoreItemReader<String>();
		itemReader.setDataStoreReader(reader);
		itemReader.setLineDataMapper(new PassThroughLineDataMapper());
		int count = 0;
		while (itemReader.read() != null) {
			count++;
		}
		assertThat(count, is(300));

		ExecutionContext context = new ExecutionContext();
		itemReader.update(context);
		assertThat(context.getLong(DataStoreItemReader.READ_POSITION), is(300l));
	}

	private void createTestData(Path path) throws IOException {
		FileSystem fs = FileSystem.get(getYarnCluster().getConfiguration());
		FSDataOutputStream out = fs.create(path);
		for (int i = 0; i < 300; i++) {
			out.writeBytes("line" + i + "\n");
		}
		out.close();
		assertTrue(fs.exists(path));
		assertThat(fs.getFileStatus(path).getLen(), greaterThan(0l));
	}

	@Override
	@Autowired(required = false)
	public void setYarnClient(YarnClient yarnClient) {
		super.setYarnClient(yarnClient);
	}

	@Configuration
	public static class Config {
	}

}
