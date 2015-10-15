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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.input.TextFileReader;
import org.springframework.data.hadoop.store.output.PartitionTextFileWriter;
import org.springframework.data.hadoop.store.output.TextFileWriter;
import org.springframework.data.hadoop.store.partition.DefaultPartitionStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class TextFileStoreFlushTests extends AbstractStoreTests {

	@org.springframework.context.annotation.Configuration
	static class Config {
		// just empty to survive without xml configs
	}

	@Test
	public void testWriteFlushAndRead() throws Exception {
		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath, null);
		writer.setSyncable(true);
		TestUtils.writeData(writer, DATA09ARRAY, false);
		writer.flush();

		TextFileReader reader = new TextFileReader(getConfiguration(), testDefaultPath, null);
		TestUtils.readDataAndAssert(reader, DATA09ARRAY);
		writer.close();
	}

	@Test
	public void testWriteFlushAndReadForPartition() throws Exception {
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
	}

	@Test
	public void testPollerCallback() throws Exception {
		TestTextFileWriter writer = new TestTextFileWriter(getConfiguration(), testDefaultPath, null);
		writer.setTaskExecutor(new ConcurrentTaskExecutor());
		writer.setTaskScheduler(new ConcurrentTaskScheduler());
		writer.setFlushTimeout(100);
		writer.afterPropertiesSet();
		writer.start();
		assertThat(writer.flushTimeoutLatch.await(2, TimeUnit.SECONDS), is(true));
		writer.close();
	}

	private class TestTextFileWriter extends TextFileWriter {

		CountDownLatch flushTimeoutLatch = new CountDownLatch(5);

		public TestTextFileWriter(Configuration configuration, Path basePath, CodecInfo codec) {
			super(configuration, basePath, codec);
		}

		@Override
		protected void flushTimeout() {
			super.flushTimeout();
			flushTimeoutLatch.countDown();
		}
	}

}
