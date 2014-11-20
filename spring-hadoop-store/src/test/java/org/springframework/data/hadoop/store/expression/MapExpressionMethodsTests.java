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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class MapExpressionMethodsTests {

	@Test
	public void testRawExpressions() {
		Map<String, Object> message = new HashMap<String, Object>();
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("timestamp", System.currentTimeMillis());
		message.put("payload", "jee");
		message.put("timestamp", System.currentTimeMillis());
		message.put("headerkey", "headervalue");
		message.put("headers", headers);

		String nowYYYYMM = new SimpleDateFormat("yyyy/MM").format(new Date());

		ExpressionParser parser = new SpelExpressionParser();
		StandardEvaluationContext context = new StandardEvaluationContext();
		PartitionKeyMethodResolver resolver = new PartitionKeyMethodResolver();
		context.addMethodResolver(resolver);
		context.addPropertyAccessor(new MapAccessor());

		assertThat(parser.parseExpression("dateFormat('yyyy/MM', headers[timestamp])").getValue(context, message, String.class), is(nowYYYYMM));
		assertThat(parser.parseExpression("dateFormat('yyyy/MM', timestamp)").getValue(context, message, String.class), is(nowYYYYMM));
		assertThat(parser.parseExpression("dateFormat('yyyy/MM', T(java.lang.System).currentTimeMillis())").getValue(context, message, String.class), is(nowYYYYMM));
		assertThat(parser.parseExpression("path('yyyy', 'MM')").getValue(context, message, String.class), is("yyyy/MM"));
		assertThat(parser.parseExpression("path('yyyy', 'MM', payload.substring(0,3))").getValue(context, message, String.class), is("yyyy/MM/jee"));
		assertThat(parser.parseExpression("headerkey").getValue(context, message, String.class), is("headervalue"));
		assertThat(parser.parseExpression("headers.timestamp").getValue(context, message, Long.class), greaterThan(0l));
		assertThat(parser.parseExpression("headers[timestamp]").getValue(context, message, Long.class), greaterThan(0l));
		assertThat(parser.parseExpression("payload").getValue(context, message, String.class), is("jee"));
	}

}
