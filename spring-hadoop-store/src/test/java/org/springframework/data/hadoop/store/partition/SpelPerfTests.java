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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Test;
import org.springframework.data.hadoop.store.expression.MessagePartitionKeyPropertyAccessor;
import org.springframework.data.hadoop.store.expression.PartitionKeyMethodResolver;
import org.springframework.data.hadoop.test.tests.Assume;
import org.springframework.data.hadoop.test.tests.TestGroup;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.StopWatch;

public class SpelPerfTests {

	private final int COUNT = 1000000;

	@Test
	public void testSpelPerf() throws IOException {
		Assume.group(TestGroup.PERFORMANCE);
		ExpressionParser parser = new SpelExpressionParser();
		StandardEvaluationContext context = new StandardEvaluationContext();

		Expression expression1 = parser.parseExpression("'data-data'");
		String value1 = expression1.getValue(context, String.class);
		assertThat(value1, is("data-data"));

		Expression expression2 = parser.parseExpression("'data-data'.split('-')[0]");
		String value2 = expression2.getValue(context, String.class);
		assertThat(value2, is("data"));

		Expression expression3 = parser.parseExpression("'data-data'.split('-')");
		String[] value3 = expression3.getValue(context, String[].class);
		assertThat(value3.length, is(2));
		assertThat(value3[0], is("data"));
		assertThat(value3[1], is("data"));

		String root4 = "data-data";
		Expression expression4 = parser.parseExpression("#root.split('-')");
		String[] value4 = expression4.getValue(context, root4, String[].class);
		assertThat(value4.length, is(2));
		assertThat(value4[0], is("data"));
		assertThat(value4[1], is("data"));

		String root5 = "data-data";
		Expression expression5 = parser.parseExpression("#root.split('-')[0]");
		String value5 = expression5.getValue(context, root5, String.class);
		assertThat(value5, is("data"));

		String[] root6 = new String[]{"data","data"};
		Expression expression6 = parser.parseExpression("#root[0]");
		String value6 = expression6.getValue(context, root6, String.class);
		assertThat(value6, is("data"));

		StopWatch sw = new StopWatch("testSpelPerf");
		sw.start("task1");
		for (int i = 0; i<COUNT; i++) {
			expression1.getValue(context);
		}
		sw.stop();

		sw.start("task2");
		for (int i = 0; i<COUNT; i++) {
			expression2.getValue(context);
		}
		sw.stop();

		sw.start("task3");
		for (int i = 0; i<COUNT; i++) {
			expression3.getValue(context);
		}
		sw.stop();

		sw.start("task4");
		for (int i = 0; i<COUNT; i++) {
			expression4.getValue(context, root4);
		}
		sw.stop();

		sw.start("task5");
		for (int i = 0; i<COUNT; i++) {
			expression5.getValue(context, root4);
		}
		sw.stop();

		sw.start("task6");
		for (int i = 0; i<COUNT; i++) {
			expression6.getValue(context, root6);
		}
		sw.stop();

		System.out.println(sw.prettyPrint());
	}

	@Test
	public void testPerfWithResolverAndAccessor() {
		Assume.group(TestGroup.PERFORMANCE);
		Message<String> message = MessageBuilder.withPayload("jee").build();

		ExpressionParser parser = new SpelExpressionParser();
		StandardEvaluationContext context = new StandardEvaluationContext();
		PartitionKeyMethodResolver resolver = new PartitionKeyMethodResolver();
		MessagePartitionKeyPropertyAccessor accessor = new MessagePartitionKeyPropertyAccessor();
		context.addMethodResolver(resolver);
		context.addPropertyAccessor(accessor);

		Expression expression1 = parser.parseExpression("payload");
		Expression expression2 = parser.parseExpression("timestamp");
		StopWatch sw = new StopWatch("testPerfWithResolverAndAccessor");
		sw.start();
		for (int i = 0; i<COUNT; i++) {
			expression1.getValue(context, message, String.class);
			expression2.getValue(context, message, Long.class);
		}
		sw.stop();
		System.out.println(sw.prettyPrint());
	}

	@Test
	public void testPerfWithNativeSpel() {
		Assume.group(TestGroup.PERFORMANCE);
		Message<String> message = MessageBuilder.withPayload("jee").build();

		ExpressionParser parser = new SpelExpressionParser();
		StandardEvaluationContext context = new StandardEvaluationContext();

		Expression expression1 = parser.parseExpression("payload");
		Expression expression2 = parser.parseExpression("headers.timestamp");
		StopWatch sw = new StopWatch("testPerfWithNativeSpel");
		sw.start();
		for (int i = 0; i<COUNT; i++) {
			expression1.getValue(context, message, String.class);
			expression2.getValue(context, message, Long.class);
		}
		sw.stop();
		System.out.println(sw.prettyPrint());
	}

}
