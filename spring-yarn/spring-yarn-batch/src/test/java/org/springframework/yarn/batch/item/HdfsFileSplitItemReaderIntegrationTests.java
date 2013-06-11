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
package org.springframework.yarn.batch.item;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Testing split file reads.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HdfsFileSplitItemReaderIntegrationTests {

	@javax.annotation.Resource(name = "yarnConfiguration")
	private Configuration configuration;

	private FileSystem fs;

	private HdfsResourceLoader resourceLoader;

	private String DATA1 = "/syarn-tmp/HdfsFileSplitItemReaderIntegrationTests/data1.txt";
	private Path PDATA1 = new Path(DATA1);

	@Before
	public void setUp() throws IOException {
		fs = FileSystem.get(configuration);
		fs.mkdirs(new Path("/syarn-tmp/HdfsFileSplitItemReaderIntegrationTests"));
		resourceLoader = new HdfsResourceLoader(fs);
	}

	@Test
	public void testWithOneReader() throws Exception {
		createDataFile(PDATA1, "data1-line-", 100);
		Resource resource = resourceLoader.getResource(DATA1);

		FileStatus fileStatus = fs.getFileStatus(PDATA1);
		assertThat(fileStatus, notNullValue());
		assertThat(fileStatus.isFile(), is(true));

		HdfsFileSplitItemReader<String> itemReader = createItemReader(resource, 0, fileStatus.getLen());

		readAndTestRange(itemReader, "data1-line-", 1, 100);
		itemReader.close();
	}

	@Test
	public void testWithTwoReaders() throws Exception {
		createDataFile(PDATA1, "data1-line-", 100);
		Resource resource = resourceLoader.getResource(DATA1);

		HdfsFileSplitItemReader<String> itemReader1 = createItemReader(resource, 0, 696);
		HdfsFileSplitItemReader<String> itemReader2 = createItemReader(resource, 697, 696);

		readAndTestRange(itemReader1, "data1-line-", 1, 51);
		readAndTestRange(itemReader2, "data1-line-", 52, 100);
		itemReader1.close();
		itemReader2.close();
	}

	@Test
	public void testWithThreeReaders() throws Exception {
		createDataFile(PDATA1, "data1-line-", 100);
		Resource resource = resourceLoader.getResource(DATA1);

		HdfsFileSplitItemReader<String> itemReader1 = createItemReader(resource, 0, 463);
		HdfsFileSplitItemReader<String> itemReader2 = createItemReader(resource, 464, 463);
		HdfsFileSplitItemReader<String> itemReader3 = createItemReader(resource, 928, 464);

		readAndTestRange(itemReader1, "data1-line-", 1, 34);
		readAndTestRange(itemReader2, "data1-line-", 35, 67);
		readAndTestRange(itemReader3, "data1-line-", 68, 100);
		itemReader1.close();
		itemReader2.close();
		itemReader3.close();
	}

	private void readAndTestRange(HdfsFileSplitItemReader<String> itemReader, String prefix, int expectedStart, int expectedEnd) throws Exception {
		String line = null;
		for (int i = expectedStart; i<= expectedEnd; i++) {
			line = itemReader.read();
			assertThat(line, is(prefix + i));
		}
		assertThat(itemReader.read(), is(nullValue()));
	}

	private void createDataFile(Path path, String prefix, int rows) throws Exception {
		FSDataOutputStream outputStream = fs.create(path, true);
		for (int i = 1; i<=rows; i++) {
			outputStream.writeBytes(prefix + i + "\n");
		}
		outputStream.close();
	}

	private HdfsFileSplitItemReader<String> createItemReader(Resource resource, long start, long length) {
		HdfsFileSplitItemReader<String> itemReader = new HdfsFileSplitItemReader<String>();
		itemReader.setResource(resource);
		itemReader.setSplitStart(start);
		itemReader.setSplitLength(length);
		itemReader.open(new ExecutionContext());
		return itemReader;
	}

}

