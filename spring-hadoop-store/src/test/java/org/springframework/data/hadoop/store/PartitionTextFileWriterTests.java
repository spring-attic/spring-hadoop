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
package org.springframework.data.hadoop.store;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.data.hadoop.store.input.TextFileReader;
import org.springframework.data.hadoop.store.output.PartitionTextFileWriter;
import org.springframework.data.hadoop.store.partition.DefaultPartitionStrategy;
import org.springframework.data.hadoop.store.partition.MessagePartitionStrategy;
import org.springframework.data.hadoop.store.partition.PartitionKeyResolver;
import org.springframework.data.hadoop.store.partition.PartitionResolver;
import org.springframework.data.hadoop.store.partition.PartitionStrategy;
import org.springframework.data.hadoop.store.strategy.naming.RollingFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.SizeRolloverStrategy;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.expression.spel.SpelCompilerMode;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class PartitionTextFileWriterTests extends AbstractStoreTests {

	@org.springframework.context.annotation.Configuration
	static class Config {
		// just empty to survive without xml configs
	}

	@Test
	public void testMapWriteReadTextOneLine() throws IOException {
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

		writer.write(dataArray[0], headers);
		writer.flush();
		writer.close();

		TextFileReader reader = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "foo/" + nowYYYYMM + "/0_hash/jee_list/10_range/bar"), null);
		TestUtils.readDataAndAssert(reader, dataArray);
	}

	@Test
	public void testMessageWriteReadTextOneLine() throws IOException {
		String expression = "headers[region] + '/' + dateFormat('yyyy/MM', headers[timestamp])";
		String[] dataArray = new String[] { DATA10 };
		MessagePartitionStrategy<String> strategy = new MessagePartitionStrategy<String>(expression,
				new StandardEvaluationContext(), new SpelExpressionParser(new SpelParserConfiguration(
						SpelCompilerMode.MIXED, null)));

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("region", "foo");
		Message<String> message = MessageBuilder.withPayload("jee").copyHeaders(headers).build();
		String nowYYYYMM = new SimpleDateFormat("yyyy/MM").format(new Date());

		PartitionTextFileWriter<Message<?>> writer =
				new PartitionTextFileWriter<Message<?>>(getConfiguration(), testDefaultPath, null, strategy);
		writer.setFileNamingStrategyFactory(new StaticFileNamingStrategy("bar"));

		writer.write(dataArray[0], message);
		writer.flush();
		writer.close();

		TextFileReader reader = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "foo/" + nowYYYYMM + "/bar"), null);
		TestUtils.readDataAndAssert(reader, dataArray);
	}

	@Test
	public void testFallbackWriter() throws IOException {
		String expression = "headers[region] + '/' + dateFormat('yyyy/MM', headers[timestamp])";
		String[] dataArray = new String[] { DATA10 };
		MessagePartitionStrategy<String> strategy = new MessagePartitionStrategy<String>(expression,
				new StandardEvaluationContext(), new SpelExpressionParser(new SpelParserConfiguration(
						SpelCompilerMode.MIXED, null)));

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("region", "foo");

		PartitionTextFileWriter<Message<?>> writer =
				new PartitionTextFileWriter<Message<?>>(getConfiguration(), testDefaultPath, null, strategy);
		writer.setFileNamingStrategyFactory(new StaticFileNamingStrategy("bar"));

		writer.write(dataArray[0], null);
		writer.flush();
		writer.close();

		TextFileReader reader = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "bar"), null);
		TestUtils.readDataAndAssert(reader, dataArray);
	}

	@Test
	public void testWriteReadManyLinesWithNamingAndRollover() throws IOException {

		String expression = "headers[region].toString() + '/' + dateFormat('yyyy/MM', headers[timestamp])";
		MessagePartitionStrategy<String> strategy = new MessagePartitionStrategy<String>(expression,
				new StandardEvaluationContext(), new SpelExpressionParser(new SpelParserConfiguration(
						SpelCompilerMode.MIXED, null)));

		PartitionTextFileWriter<Message<?>> writer =
				new PartitionTextFileWriter<Message<?>>(getConfiguration(), testDefaultPath, null, strategy);

		writer.setFileNamingStrategyFactory(new RollingFileNamingStrategy());
		writer.setRolloverStrategyFactory(new SizeRolloverStrategy(40));

		for (String data : DATA09ARRAY) {
			Map<String, Object> headers = new HashMap<String, Object>();
			headers.put("region", "foo");
			Message<String> message = MessageBuilder.withPayload(data).copyHeaders(headers).build();
			writer.write(data, message);
		}
		writer.flush();
		writer.close();

		String nowYYYYMM = new SimpleDateFormat("yyyy/MM").format(new Date());

		TextFileReader reader1 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "foo/" + nowYYYYMM + "/0"), null);
		List<String> splitData1 = TestUtils.readData(reader1);

		TextFileReader reader2 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "foo/" + nowYYYYMM + "/1"), null);
		List<String> splitData2 = TestUtils.readData(reader2);

		TextFileReader reader3 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "foo/" + nowYYYYMM + "/2"), null);
		List<String> splitData3 = TestUtils.readData(reader3);

		assertThat(splitData1.size() + splitData2.size() + splitData3.size(), is(DATA09ARRAY.length));
	}

	@Test
	public void testCustomPartitioningKeys() throws IOException {
		String[] dataArray1 = new String[] { "customer1-1", "customer1-2", "customer1-3" };
		String[] dataArray2 = new String[] { "customer2-1", "customer2-2", "customer2-3" };
		String[] dataArray3 = new String[] { "customer3-1", "customer3-2", "customer3-3" };
		CustomerPartitionStrategy strategy = new CustomerPartitionStrategy();
		PartitionTextFileWriter<String> writer =
				new PartitionTextFileWriter<String>(getConfiguration(), testDefaultPath, null, strategy);

		writer.write(dataArray1[0], "customer1");
		writer.write(dataArray1[1], "customer1");
		writer.write(dataArray1[2], "customer1");
		writer.write(dataArray2[0], "customer2");
		writer.write(dataArray2[1], "customer2");
		writer.write(dataArray2[2], "customer2");
		writer.write(dataArray3[0], "customer3");
		writer.write(dataArray3[1], "customer3");
		writer.write(dataArray3[2], "customer3");
		writer.flush();
		writer.close();

		// /tmp/TextFilePartitionedWriterTests/default/customer1
		TextFileReader reader1 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "customer1"), null);
		TestUtils.readDataAndAssert(reader1, dataArray1);

		// /tmp/TextFilePartitionedWriterTests/default/customer2
		TextFileReader reader2 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "customer2"), null);
		TestUtils.readDataAndAssert(reader2, dataArray2);

		// /tmp/TextFilePartitionedWriterTests/default/customer3
		TextFileReader reader3 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "customer3"), null);
		TestUtils.readDataAndAssert(reader3, dataArray3);
	}

	@Test
	public void testCustomPartitionKeyResolving() throws IOException {
		String[] dataArray1 = new String[] { "customer1-1", "customer1-2", "customer1-3" };
		String[] dataArray2 = new String[] { "customer2-1", "customer2-2", "customer2-3" };
		String[] dataArray3 = new String[] { "customer3-1", "customer3-2", "customer3-3" };
		CustomerPartitionStrategy strategy = new CustomerPartitionStrategy();
		PartitionTextFileWriter<String> writer =
				new PartitionTextFileWriter<String>(getConfiguration(), testDefaultPath, null, strategy);

		writer.write(dataArray1[0]);
		writer.write(dataArray1[1]);
		writer.write(dataArray1[2]);
		writer.write(dataArray2[0]);
		writer.write(dataArray2[1]);
		writer.write(dataArray2[2]);
		writer.write(dataArray3[0]);
		writer.write(dataArray3[1]);
		writer.write(dataArray3[2]);
		writer.flush();
		writer.close();

		// /tmp/TextFilePartitionedWriterTests/default/customer1
		TextFileReader reader1 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "customer1"), null);
		TestUtils.readDataAndAssert(reader1, dataArray1);

		// /tmp/TextFilePartitionedWriterTests/default/customer2
		TextFileReader reader2 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "customer2"), null);
		TestUtils.readDataAndAssert(reader2, dataArray2);

		// /tmp/TextFilePartitionedWriterTests/default/customer3
		TextFileReader reader3 = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "customer3"), null);
		TestUtils.readDataAndAssert(reader3, dataArray3);
	}

	@Test
	public void testWritersCleaned() throws Exception {
		String expression = "path(region)";
		String[] dataArray = new String[] { DATA10 };
		DefaultPartitionStrategy<String> strategy = new DefaultPartitionStrategy<String>(expression);

		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("region", "reg1");

		PartitionTextFileWriter<Map<String, Object>> writer =
				new PartitionTextFileWriter<Map<String, Object>>(getConfiguration(), testDefaultPath, null, strategy);
		writer.setFileNamingStrategyFactory(new StaticFileNamingStrategy("bar"));

		writer.write(dataArray[0], headers);
		writer.flush();

		Map<Path, DataStoreWriter<String>> writers = TestUtils.readField("writers", writer);
		assertThat(writers.size(), is(1));

		headers.put("region", "reg2");
		writer.write(dataArray[0], headers);
		writer.flush();

		writers = TestUtils.readField("writers", writer);
		assertThat(writers.size(), is(2));

		DataStoreWriter<String> toclose = writers.values().iterator().next();
		toclose.close();
		writer.flush();
		writers = TestUtils.readField("writers", writer);
		assertThat(writers.size(), is(1));

		writer.close();

		TextFileReader reader = new TextFileReader(getConfiguration(), new Path(testDefaultPath, "reg1/bar"), null);
		TestUtils.readDataAndAssert(reader, dataArray);

		writers = TestUtils.readField("writers", writer);
		assertThat(writers.size(), is(0));

	}

	private static class CustomerPartitionStrategy implements PartitionStrategy<String, String> {

		CustomerPartitionResolver partitionResolver = new CustomerPartitionResolver();
		CustomerPartitionKeyResolver keyResolver = new CustomerPartitionKeyResolver();

		@Override
		public PartitionResolver<String> getPartitionResolver() {
			return partitionResolver;
		}

		@Override
		public PartitionKeyResolver<String, String> getPartitionKeyResolver() {
			return keyResolver;
		}
	}

	private static class CustomerPartitionResolver implements PartitionResolver<String> {

		@Override
		public Path resolvePath(String partitionKey) {
			return new Path(partitionKey);
		}
	}

	private static class CustomerPartitionKeyResolver implements PartitionKeyResolver<String, String> {

		@Override
		public String resolvePartitionKey(String entity) {
			if (entity.startsWith("customer1")) {
				return "customer1";
			} else if (entity.startsWith("customer2")) {
				return "customer2";
			} else if (entity.startsWith("customer3")) {
				return "customer3";
			}
			return null;
		}
	}

}
