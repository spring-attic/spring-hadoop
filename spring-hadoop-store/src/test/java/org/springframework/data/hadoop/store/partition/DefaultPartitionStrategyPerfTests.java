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
package org.springframework.data.hadoop.store.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.data.hadoop.store.AbstractStoreTests;
import org.springframework.data.hadoop.store.output.PartitionTextFileWriter;
import org.springframework.data.hadoop.store.output.TextFileWriter;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.tests.Assume;
import org.springframework.data.hadoop.test.tests.TestGroup;
import org.springframework.expression.spel.SpelCompilerMode;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.StopWatch;

/**
 * Performance tests for store writers.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class DefaultPartitionStrategyPerfTests extends AbstractStoreTests {

	private final int COUNT = 50000;
//	private final int COUNT = 1000000;
//	private final int COUNT = 1000;

	/**
	 * Getting baseline perf number for using a single writer and
	 * no partitioning. We can compare all other tests to this number.
	 */
	@Test
	public void testBaseline() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		StopWatch sw = new StopWatch("testBaseline");
		sw.start();
		TextFileWriter writer = new TextFileWriter(getConfiguration(), testDefaultPath, null);
		for (int i = 0; i<COUNT; i++) {
			writer.write(DATA10);
		}
		sw.stop();
		writer.close();
		System.out.println(sw.prettyPrint());
	}

	/**
	 * Using a dummy partition expression which simply makes sure that
	 * we are using an expression for partitioning. We only get one
	 * partition with this.
	 */
	@Test
	public void testDummyPartitionExpressionStringConcat() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "'dummy' + '/' + 'partition'";
		testPerformance("testDummyPartitionExpressionStringConcat", expression);
	}

	@Test
	public void testDummyPartitionExpressionIntSum() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "1 + 1";
		testPerformance("testDummyPartitionExpressionIntSum", expression);
	}

	/**
	 * Same as {@link #testDummyPartitionExpressionStringConcat()} but using a path
	 * function in an expression.
	 */
	@Test
	public void testDummyPartitionWithPathFunction() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "path('dummy','partition')";
		testPerformance("testDummyPartitionWithPathFunction", expression);
	}

	@Test
	public void testPartitionWithHashRange() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "range(1,{3,5,10})";
		testPerformance("testPartitionWithHashRange", expression);
	}

	@Test
	public void testLargeList() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "list(payload.split('\u0001')[0],{{'1TO5','APP1','APP2','APP3','APP4','APP5'},{'6TO10','APP6','APP7','APP8','APP9','APP10'}})";

		ArrayList<Map<String, Object>> messages = new ArrayList<Map<String,Object>>();
		for (int i = 0; i<10; i++) {
			String payload = "APP" + (i+1) + "\u0001" + "somedata";
			Map<String, Object> message = new HashMap<String, Object>();
			message.put("payload", payload);
			messages.add(message);
		}

		testPerformance("testLargeList", expression, messages);
	}

	@Test
	public void testListWithConstant() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "list('APP1',{{'1TO5','APP1'}})";
		testPerformance("testListWithConstant", expression);
	}

	@Test
	public void testPathAndListWithConstant() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "path(list('APP1',{{'1TO5','APP1'}}))";
		testPerformance("testPathAndListWithConstant", expression);
	}

	@Test
	public void testWithPayloadSplit() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "payload.split('\u0001')[0]";

		ArrayList<Map<String, Object>> messages = new ArrayList<Map<String,Object>>();
		for (int i = 0; i<1; i++) {
			String payload = "APP" + (i+1) + "\u0001" + "somedata";
			Map<String, Object> message = new HashMap<String, Object>();
			message.put("payload", payload);
			messages.add(message);
		}

		testPerformance("testWithPayloadSplit", expression, messages);
	}

	@Test
	public void testListWithPayloadSplit() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "list(payload.split('\u0001')[0],{{'1TO5','APP1'}})";

		ArrayList<Map<String, Object>> messages = new ArrayList<Map<String,Object>>();
		for (int i = 0; i<1; i++) {
			String payload = "APP" + (i+1) + "\u0001" + "somedata";
			Map<String, Object> message = new HashMap<String, Object>();
			message.put("payload", payload);
			messages.add(message);
		}

		testPerformance("testListWithPayloadSplit", expression, messages);
	}

	@Test
	public void testListWithPayload() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "list(payload,{{'1TO5','APP1'}})";

		ArrayList<Map<String, Object>> messages = new ArrayList<Map<String,Object>>();
		for (int i = 0; i<1; i++) {
			String payload = "APP" + (i+1) + "\u0001" + "somedata";
			Map<String, Object> message = new HashMap<String, Object>();
			message.put("payload", payload);
			messages.add(message);
		}

		testPerformance("testListWithPayload", expression, messages);
	}

	@Test
	public void testPartitionWithPathAndHashRange() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "path(range(1,{3,5,10}))";
		testPerformance("testPartitionWithPathAndHashRange", expression);
	}

	/**
	 * Using an internal dataFormat function for partition path. Uses only
	 * one partition.
	 */
	@Test
	public void testPartitioningWithDateFormat() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "dateFormat('yyyy/MM',timestamp)";
		testPerformance("testPartitioningWithDateFormat", expression);
	}

	/**
	 * Using dateFormat, list function with a stripping list id from a payload.
	 */
	@Test
	public void testDateFormatAndListAndPayloadSplit() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "path(dateFormat('yyyy/MM/dd',timestamp),list(payload.split('\u0001')[0],{{'1TO5','APP1','APP2','APP3','APP4','APP5'},{'6TO10','APP6','APP7','APP8','APP9','APP10'}}))";

		ArrayList<Map<String, Object>> messages = new ArrayList<Map<String,Object>>();
		for (int i = 0; i<10; i++) {
			String payload = "APP" + (i+1) + "\u0001" + "somedata";
			Map<String, Object> message = new HashMap<String, Object>();
			message.put("payload", payload);
			message.put("timestamp", 0);
			messages.add(message);
		}

		testPerformance("testDateFormatAndListAndPayloadSplit", expression, messages);
	}

	@Test
	public void testUsePayload1() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "path(payload.split('\u0001')[0])";

		ArrayList<Map<String, Object>> messages = new ArrayList<Map<String,Object>>();
		for (int i = 0; i<10; i++) {
			String payload = "APP" + (i+1) + "\u0001" + "somedata";
			Map<String, Object> message = new HashMap<String, Object>();
			message.put("payload", payload);
			messages.add(message);
		}

		testPerformance("testUsePayload1", expression, messages);
	}

	@Test
	public void testUsePayload2() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "path(payload.split('-')[0])";

		ArrayList<Map<String, Object>> messages = new ArrayList<Map<String,Object>>();
		for (int i = 0; i<10; i++) {
			String payload = "APP" + (i+1) + "\u0001" + "somedata";
			Map<String, Object> message = new HashMap<String, Object>();
			message.put("payload", payload);
			messages.add(message);
		}

		testPerformance("testUsePayload2", expression, messages);
	}

	@Test
	public void testUsePayload3() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "path(payload)";

		ArrayList<Map<String, Object>> messages = new ArrayList<Map<String,Object>>();
		for (int i = 0; i<10; i++) {
			String payload = "APP" + (i+1) + "\u0001" + "somedata";
			Map<String, Object> message = new HashMap<String, Object>();
			message.put("payload", payload);
			messages.add(message);
		}

		testPerformance("testUsePayload3", expression, messages);
	}

	@Test
	public void testUsePayload4() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		String expression = "path(T(org.springframework.util.StringUtils).split(payload,'-')[0])";

		ArrayList<Map<String, Object>> messages = new ArrayList<Map<String,Object>>();
		for (int i = 0; i<10; i++) {
			String payload = "APP" + (i+1) + "-somedata";
			Map<String, Object> message = new HashMap<String, Object>();
			message.put("payload", payload);
			messages.add(message);
		}

		testPerformance("testUsePayload4", expression, messages);
	}

	private void testPerformance(String name, String expression) throws IOException {
		ArrayList<Map<String, Object>> messages = new ArrayList<Map<String,Object>>(1);
		Map<String, Object> message = new HashMap<String, Object>();
		message.put("payload", "dummy");
		message.put("timestamp", 0);
		messages.add(message);
		testPerformance(name, expression, messages);
	}

	private void testPerformance(String name, String expression, List<Map<String, Object>> messages) throws IOException {
		// customexecutor
		DefaultPartitionStrategy<String> strategy1 = new DefaultPartitionStrategy<String>(expression,
				new StandardEvaluationContext());
		PartitionTextFileWriter<Map<String, Object>> writer1 = new PartitionTextFileWriter<Map<String, Object>>(getConfiguration(),
				new Path(testDefaultPath, "1"), null, strategy1);

		// reflection
		DefaultPartitionStrategy<String> strategy2 = new DefaultPartitionStrategy<String>(expression,
				new StandardEvaluationContext(), new SpelExpressionParser(new SpelParserConfiguration(SpelCompilerMode.OFF, null)));
		PartitionTextFileWriter<Map<String, Object>> writer2 = new PartitionTextFileWriter<Map<String, Object>>(getConfiguration(),
				new Path(testDefaultPath, "2"), null, strategy2);

		// compile
		DefaultPartitionStrategy<String> strategy3 = new DefaultPartitionStrategy<String>(expression,
				new StandardEvaluationContext(), new SpelExpressionParser(new SpelParserConfiguration(SpelCompilerMode.IMMEDIATE, null)));
		PartitionTextFileWriter<Map<String, Object>> writer3 = new PartitionTextFileWriter<Map<String, Object>>(getConfiguration(),
				new Path(testDefaultPath, "3"), null, strategy3);

		StopWatch sw = new StopWatch(name);

		sw.start("customexecutor");
		for (int i = 0; i<COUNT; i++) {
			writer1.write(DATA10, messages.get(i%messages.size()));
		}
		sw.stop();
		writer1.close();

		sw.start("reflection");
		for (int i = 0; i<COUNT; i++) {
			writer2.write(DATA10, messages.get(i%messages.size()));
		}
		sw.stop();
		writer2.close();

		sw.start("compile");
		for (int i = 0; i<COUNT; i++) {
			writer3.write(DATA10, messages.get(i%messages.size()));
		}
		sw.stop();
		writer3.close();

		System.out.println("Expression: " + expression);
		System.out.println(sw.prettyPrint());
	}

	@org.springframework.context.annotation.Configuration
	static class Config {
		// just empty to survive without xml configs
	}

}
