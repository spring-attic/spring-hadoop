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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.expression.spel.SpelCompilerMode;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Tests for {@link MessagePartitionStrategy}.
 *
 * @author Janne Valkealahti
 *
 */
public class MessagePartitionStrategyTests {

	@Test
	public void testCustomResolverLogic() {
		String expression = "headers[region] + '/' + dateFormat('yyyy/MM', headers[timestamp])";
		MessagePartitionStrategy<String> strategy = new MessagePartitionStrategy<String>(expression,
				new StandardEvaluationContext(), new SpelExpressionParser(new SpelParserConfiguration(
						SpelCompilerMode.OFF, null)));
		Message<String> message = MessageBuilder.withPayload("jee").setHeader("region", "foo").build();
		assertStrategy(strategy, message);
	}

	@Test
	public void testNonCompileMode() {
		String expression = "headers[region] + '/' + dateFormat('yyyy/MM', headers[timestamp])";
		MessagePartitionStrategy<String> strategy = new MessagePartitionStrategy<String>(expression,
				new StandardEvaluationContext(), new SpelExpressionParser(new SpelParserConfiguration(
						SpelCompilerMode.OFF, null)));
		Message<String> message = MessageBuilder.withPayload("jee").setHeader("region", "foo").build();
		assertStrategy(strategy, message);
	}

	@Test
	public void testCompileMode() {
		String expression = "headers[region] + '/' + dateFormat('yyyy/MM', headers[timestamp])";
		MessagePartitionStrategy<String> strategy = new MessagePartitionStrategy<String>(expression,
				new StandardEvaluationContext(), new SpelExpressionParser(new SpelParserConfiguration(
						SpelCompilerMode.IMMEDIATE, null)));
		Message<String> message = MessageBuilder.withPayload("jee").setHeader("region", "foo").build();
		assertStrategy(strategy, message);
	}

	private static void assertStrategy(MessagePartitionStrategy<String> strategy, Message<String> message) {
		Path resolvedPath = strategy.getPartitionResolver().resolvePath(message);
		assertThat(resolvedPath, notNullValue());
		Message<?> resolvedPartitionKey = strategy.getPartitionKeyResolver().resolvePartitionKey("foo");
		assertThat(resolvedPartitionKey, notNullValue());
	}

}
