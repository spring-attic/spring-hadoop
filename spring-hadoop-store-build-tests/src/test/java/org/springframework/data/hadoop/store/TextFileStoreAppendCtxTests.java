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

import java.io.IOException;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.store.input.TextFileReader;
import org.springframework.data.hadoop.store.output.TextFileWriter;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests for reading and writing text using context configuration.
 *
 * @author Janne Valkealahti
 * @author liu jiong
 *
 */
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class TextFileStoreAppendCtxTests extends AbstractStoreTests {

	@Autowired
	private ApplicationContext context;

	@Test
	public void testWriteReadManyLines() throws IOException, InterruptedException {
		TextFileWriter writer = context.getBean("writer", TextFileWriter.class);

		TestUtils.writeData(writer, DATA09ARRAY, false, false);
		writer.flush();

		TextFileReader reader = new TextFileReader(getConfiguration(), writer.getPath(), null);
		TestUtils.readDataAndAssert(reader, DATA09ARRAY);
		writer.close();
		reader.close();
	}

}
