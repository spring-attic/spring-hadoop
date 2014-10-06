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
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.data.hadoop.store.input.DelimitedTextFileReader;
import org.springframework.data.hadoop.store.output.DelimitedTextFileWriter;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class DelimitedTextFileStoreTests extends AbstractStoreTests {

	@org.springframework.context.annotation.Configuration
	static class Config {
		// just empty to survive without xml configs
	}

	@Test
	public void testWriteReadTextOneDelimitedLine() throws IOException {

		List<List<String>> data = new ArrayList<List<String>>();
		data.add(Arrays.asList(DATA09ARRAY));

		DelimitedTextFileWriter writer = new DelimitedTextFileWriter(getConfiguration(), testDefaultPath, null);
		TestUtils.writeData(writer, data);

		DelimitedTextFileReader reader = new DelimitedTextFileReader(getConfiguration(), testDefaultPath, null);
		List<List<String>> list = TestUtils.readDataList(reader);
		assertThat(list, notNullValue());
		assertThat(list.size(), is(1));
		assertThat(list.get(0).size(), is(10));
	}

	@Test
	public void testWriteReadTextManyDelimitedLine() throws IOException {

		List<List<String>> data = new ArrayList<List<String>>();
		data.add(Arrays.asList(DATA09ARRAY));
		data.add(Arrays.asList(DATA09ARRAY));
		data.add(Arrays.asList(DATA09ARRAY));

		DelimitedTextFileWriter writer = new DelimitedTextFileWriter(getConfiguration(), testDefaultPath, null);
		TestUtils.writeData(writer, data);

		DelimitedTextFileReader reader = new DelimitedTextFileReader(getConfiguration(), testDefaultPath, null);
		List<List<String>> list = TestUtils.readDataList(reader);
		assertThat(list, notNullValue());
		assertThat(list.size(), is(3));
		assertThat(list.get(0).size(), is(10));
		assertThat(list.get(1).size(), is(10));
		assertThat(list.get(2).size(), is(10));
	}

}
