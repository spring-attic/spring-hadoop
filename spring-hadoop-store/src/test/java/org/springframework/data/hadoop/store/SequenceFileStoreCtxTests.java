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

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.store.input.TextSequenceFileReader;
import org.springframework.data.hadoop.store.output.TextSequenceFileWriter;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests for reading and writing text using context configuration.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class SequenceFileStoreCtxTests extends AbstractStoreTests {

	@Autowired
	private ApplicationContext context;

	@Test
	public void testWriteReadManyLines() throws IOException, InterruptedException {

		TextSequenceFileWriter writer = context.getBean("writer", TextSequenceFileWriter.class);
		assertNotNull(writer);

		TestUtils.writeData(writer, new String[] { DATA10 }, false);
		Thread.sleep(3000);
		TestUtils.writeData(writer, new String[] { DATA11 }, false);
		Thread.sleep(3000);
		TestUtils.writeData(writer, new String[] { DATA12 }, true);
		Thread.sleep(3000);

		TextSequenceFileReader reader1 = new TextSequenceFileReader(getConfiguration(), new Path(testDefaultPath, "0"), null);
		List<String> splitData1 = TestUtils.readData(reader1);

		TextSequenceFileReader reader2 = new TextSequenceFileReader(getConfiguration(), new Path(testDefaultPath, "1"), null);
		List<String> splitData2 = TestUtils.readData(reader2);

		TextSequenceFileReader reader3 = new TextSequenceFileReader(getConfiguration(), new Path(testDefaultPath, "2"), null);
		List<String> splitData3 = TestUtils.readData(reader3);

		assertThat(splitData1.size() + splitData2.size() + splitData3.size(), is(3));
	}

}
