/*
 * Copyright 2015 the original author or authors.
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.data.hadoop.store.input.TextFileReader;
import org.springframework.data.hadoop.store.output.PartitionTextFileWriter;
import org.springframework.data.hadoop.store.output.TextFileWriter;
import org.springframework.data.hadoop.store.partition.DefaultPartitionStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class TextFileStoreSyncTests extends AbstractStoreTests {

	@org.springframework.context.annotation.Configuration
	static class Config {
		// just empty to survive without xml configs
	}

	@Test
	public void testWriteSyncReadBeforeClosing() throws IOException {
		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath, null);
		writer.setSyncable(true);
		TestUtils.writeData(writer, DATA09ARRAY, false);

		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath, null);
		TestUtils.readDataAndAssert(reader, DATA09ARRAY);
		writer.close();
	}

	@Test
	public void testPartitionWriteSyncReadBeforeClosing() throws IOException {
		String expression = "path(region,dateFormat('yyyy/MM',timestamp),hash(region,1),list(region,{{'jee','foo'}}),range(range,{10}))";
		String[] dataArray = new String[] { DATA10 };
		DefaultPartitionStrategy<String> strategy = new DefaultPartitionStrategy<String>(expression);

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("region", "foo");
		headers.put("range", 10);
		headers.put("timestamp", System.currentTimeMillis());
		String nowYYYYMM = new SimpleDateFormat("yyyy/MM").format(new Date());

		PartitionTextFileWriter<Map<String, Object>> writer =
				new PartitionTextFileWriter<Map<String, Object>>(getConfiguration(), testDefaultPath, null, strategy);
		writer.setFileNamingStrategyFactory(new StaticFileNamingStrategy("bar"));
		writer.setSyncable(true);

		writer.write(dataArray[0], headers);
		writer.flush();

		TextFileReader reader = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "foo/" + nowYYYYMM + "/0_hash/jee_list/10_range/bar"), null);
		TestUtils.readDataAndAssert(reader, dataArray);
		writer.close();
		reader.close();
	}

}
