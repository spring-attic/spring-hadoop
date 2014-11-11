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
package org.springframework.data.hadoop.store.expression;

import org.junit.Test;
import org.springframework.expression.PropertyAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

public class MessagePartitionKeyPropertyAccessorTests extends AbstractExpressionTests {

	@Test
	public void testCompilables() throws Exception {
		MessagePartitionKeyMethodResolver resolver = new MessagePartitionKeyMethodResolver();
		MessagePartitionKeyPropertyAccessor accessor = new MessagePartitionKeyPropertyAccessor();
		Message<String> rootObject = MessageBuilder.withPayload("foo-bar").setHeader("foo", "bar")
				.setHeader("onestring", "1").setHeader("oneint", 1).build();
		Long timestamp = rootObject.getHeaders().getTimestamp();

		assertExpression((PropertyAccessor)null, "payload", "foo-bar", true);
		assertExpression((PropertyAccessor)null, rootObject, "'foo' + 'bar'", "foobar", true);
		assertExpression((PropertyAccessor)null, rootObject, "headers.timestamp", timestamp.toString(), true);
		assertExpression(accessor, "payload", "foo-bar", true);
		assertExpression(accessor, rootObject, "headers.timestamp", timestamp.toString(), true);
		assertExpression(accessor, rootObject, "timestamp", timestamp.toString(), true);
		assertExpression(accessor, rootObject, "foo", "bar", true);

		assertExpression(accessor, rootObject,
				"T(Long).valueOf(headers.timestamp).toString() + '/' + T(Long).valueOf(headers.timestamp).toString()",
				timestamp.toString() + "/" + timestamp.toString(), true);

		assertExpression(accessor, rootObject,
				"T(Long).valueOf(timestamp).toString() + '/' + T(Long).valueOf(timestamp).toString()",
				timestamp.toString() + "/" + timestamp.toString(), true);

		assertExpression(accessor, rootObject,
				"T(Integer).valueOf(oneint).toString() + '/' + T(Integer).valueOf(oneint).toString()",
				"1/1", true);

		assertExpression(accessor, rootObject, "oneint", "1", true);
		assertExpression(accessor, rootObject, "onestring", "1", true);

		assertExpression(accessor, rootObject, "foo + foo", "barbar", true);
		assertExpression(accessor, rootObject, "foo + onestring", "bar1", true);
		assertExpression(accessor, rootObject, "foo + '/' + foo", "bar/bar", true);
		assertExpression(resolver, accessor, false, rootObject, "path(foo,foo)", "bar/bar", true);

		assertExpression(resolver, null, false, rootObject, "path('foo','bar')", "foo/bar", true);
		assertExpression(resolver, null, false, rootObject, "path(headers.timestamp)", timestamp.toString(), true);
		assertExpression(resolver, null, false, rootObject, "path(headers.timestamp,headers.timestamp)",
				timestamp.toString() + "/" + timestamp.toString(), true);

	}

	@Test
	public void testNonCompilables() throws Exception {
		MessagePartitionKeyPropertyAccessor accessor = new MessagePartitionKeyPropertyAccessor();
		Message<String> rootObject = MessageBuilder.withPayload("foo-bar").setHeader("foo", "bar")
				.setHeader("onestring", "1").setHeader("oneint", 1).build();
		Long timestamp = rootObject.getHeaders().getTimestamp();

		assertExpression((PropertyAccessor) null, rootObject, "headers.timestamp + '/' + headers.timestamp",
				timestamp.toString() + "/" + timestamp.toString(), false);
		assertExpression(accessor, rootObject, "timestamp + '/' + timestamp",
				timestamp.toString() + "/" + timestamp.toString(), false);
		assertExpression(accessor, rootObject, "timestamp + '/' + foo", timestamp.toString() + "/bar", false);
		assertExpression(accessor, rootObject, "foo + oneint", "bar1", false);
	}

}
