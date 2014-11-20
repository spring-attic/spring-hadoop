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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.expression.PropertyAccessor;

public class MapPartitionKeyPropertyAccessorTests extends AbstractExpressionTests {

	@Test
	public void testCompilables() throws Exception {
		PartitionKeyMethodResolver resolver = new PartitionKeyMethodResolver();
		MapPartitionKeyPropertyAccessor accessor = new MapPartitionKeyPropertyAccessor();
		Long timestamp = System.currentTimeMillis();
		Map<String, Object> rootObject = new HashMap<String, Object>();
		Map<String, Object> headers = new HashMap<String, Object>();
		rootObject.put("onestring", "1");
		rootObject.put("oneint", 1);
		headers.put("timestamp", timestamp);
		rootObject.put("foo", "bar");
		rootObject.put("payload", "foo-bar");
		rootObject.put("timestamp", timestamp);
		rootObject.put("headers", headers);


		assertExpression((PropertyAccessor)null, "payload", "foo-bar", true);
		assertExpression((PropertyAccessor)null, rootObject, "'foo' + 'bar'", "foobar", true);
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
		assertExpression(resolver, accessor, false, rootObject, "path(headers.timestamp)", timestamp.toString(), true);
		assertExpression(resolver, accessor, false, rootObject, "path(headers.timestamp,headers.timestamp)",
				timestamp.toString() + "/" + timestamp.toString(), true);

	}


}
