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
import org.springframework.data.hadoop.test.tests.Assume;
import org.springframework.data.hadoop.test.tests.TestGroup;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

public class MessageExpressionMethodsTests {

	@Test
	public void testRawExpressions() {
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("headerkey", "headervalue");
		Message<String> message = MessageBuilder.withPayload("jee").copyHeaders(headers).build();
		Message<Object> wrappedRootObject = new MessageExpressionMethods.MessageWrappedMessage(message);
		String nowYYYYMM = new SimpleDateFormat("yyyy/MM").format(new Date());
		String nowYYYY = new SimpleDateFormat("yyyy").format(new Date());

		ExpressionParser parser = new SpelExpressionParser();
		StandardEvaluationContext context = new StandardEvaluationContext();
		MessagePartitionKeyMethodResolver resolver = new MessagePartitionKeyMethodResolver();
		MessagePartitionKeyPropertyAccessor accessor = new MessagePartitionKeyPropertyAccessor();
		context.addMethodResolver(resolver);
		context.addPropertyAccessor(accessor);

		assertThat(parser.parseExpression("dateFormat('yyyy/MM')").getValue(context, wrappedRootObject, String.class), is(nowYYYYMM));
		assertThat(parser.parseExpression("dateFormat('yyyy/MM', headers[timestamp])").getValue(context, message, String.class), is(nowYYYYMM));
		assertThat(parser.parseExpression("dateFormat('yyyy/MM', timestamp)").getValue(context, message, String.class), is(nowYYYYMM));
		assertThat(parser.parseExpression("dateFormat('yyyy/MM', T(java.lang.System).currentTimeMillis())").getValue(context, message, String.class), is(nowYYYYMM));
		assertThat(parser.parseExpression("path(dateFormat('yyyy'),dateFormat('MM'))").getValue(context, wrappedRootObject, String.class), is(nowYYYYMM));
		assertThat(parser.parseExpression("path('yyyy', 'MM')").getValue(context, message, String.class), is("yyyy/MM"));
		assertThat(parser.parseExpression("path('yyyy', 'MM', payload.substring(0,3))").getValue(context, message, String.class), is("yyyy/MM/jee"));
		assertThat(parser.parseExpression("dateFormat('yyyy') + '/' + dateFormat('MM')").getValue(context, wrappedRootObject, String.class), is(nowYYYYMM));
		assertThat(parser.parseExpression("headerkey").getValue(context, message, String.class), is("headervalue"));
		assertThat(parser.parseExpression("path(dateFormat('yyyy'), headerkey)").getValue(context, wrappedRootObject, String.class), is(nowYYYY + "/headervalue"));
		assertThat(parser.parseExpression("headers.timestamp").getValue(context, message, Long.class), greaterThan(0l));
		assertThat(parser.parseExpression("headers[timestamp]").getValue(context, message, Long.class), greaterThan(0l));
		assertThat(parser.parseExpression("payload").getValue(context, message, String.class), is("jee"));
	}

	@Test
	public void testWrapper() {
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("headerkey", "headervalue");
		Message<String> message = MessageBuilder.withPayload("jee").copyHeaders(headers).build();
		String nowYYYYMM = new SimpleDateFormat("yyyy/MM").format(new Date());
		String nowYYYY = new SimpleDateFormat("yyyy").format(new Date());

		String defaultFormat = "yyyy-MM-dd";
		String stringDateDefaultFormat = new SimpleDateFormat(defaultFormat).format(new Date());
		String customDateFormat = "yyyy-MM-dd";
		String stringDateCustomFormat = new SimpleDateFormat(customDateFormat).format(new Date());

		Message<String> dateMessage = MessageBuilder.withPayload(new SimpleDateFormat(defaultFormat).format(new Date())).build();

		ExpressionParser parser = new SpelExpressionParser();

		MessageExpressionMethods methods = new MessageExpressionMethods(new StandardEvaluationContext(), true, true);
		assertThat(methods.getValue(parser.parseExpression("dateFormat('yyyy/MM')"), message, String.class), is(nowYYYYMM));
		assertThat(methods.getValue(parser.parseExpression("dateFormat('yyyy/MM', headers[timestamp])"), message, String.class), is(nowYYYYMM));
		assertThat(methods.getValue(parser.parseExpression("dateFormat('yyyy/MM', timestamp)"), message, String.class), is(nowYYYYMM));
		assertThat(methods.getValue(parser.parseExpression("dateFormat('yyyy/MM', T(java.lang.System).currentTimeMillis())"), message, String.class), is(nowYYYYMM));
		assertThat(methods.getValue(parser.parseExpression("path(dateFormat('yyyy'),dateFormat('MM'))"), message, String.class), is(nowYYYYMM));
		assertThat(methods.getValue(parser.parseExpression("path('yyyy', 'MM')"), message, String.class), is("yyyy/MM"));
		assertThat(methods.getValue(parser.parseExpression("path('yyyy', 'MM', payload.substring(0,3))"), message, String.class), is("yyyy/MM/jee"));
		assertThat(methods.getValue(parser.parseExpression("dateFormat('yyyy') + '/' + dateFormat('MM')"), message, String.class), is(nowYYYYMM));
		assertThat(methods.getValue(parser.parseExpression("headerkey"), message, String.class), is("headervalue"));
		assertThat(methods.getValue(parser.parseExpression("path(dateFormat('yyyy'), headerkey)"), message, String.class), is(nowYYYY + "/headervalue"));
		assertThat(methods.getValue(parser.parseExpression("headers.timestamp"), message, Long.class), greaterThan(0l));
		assertThat(methods.getValue(parser.parseExpression("headers[timestamp]"), message, Long.class), greaterThan(0l));
		assertThat(methods.getValue(parser.parseExpression("payload"), message, String.class), is("jee"));
		assertThat(methods.getValue(parser.parseExpression("dateFormat('yyyy/MM', '" + stringDateDefaultFormat + "')"), message, String.class), is(nowYYYYMM));
		assertThat(methods.getValue(parser.parseExpression("dateFormat('yyyy/MM', '" + stringDateCustomFormat + "', '" + customDateFormat + "')"), message, String.class), is(nowYYYYMM));
		assertThat(methods.getValue(parser.parseExpression("dateFormat('yyyy/MM', payload.substring(0,10))"), dateMessage, String.class), is(nowYYYYMM));
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
		for (int i = 0; i<1000000; i++) {
			expression1.getValue(context, message, String.class);
			expression2.getValue(context, message, Long.class);
		}
	}

	@Test
	public void testPerfWithNativeSpel() {
		Assume.group(TestGroup.PERFORMANCE);
		Message<String> message = MessageBuilder.withPayload("jee").build();

		ExpressionParser parser = new SpelExpressionParser();
		StandardEvaluationContext context = new StandardEvaluationContext();

		Expression expression1 = parser.parseExpression("payload");
		Expression expression2 = parser.parseExpression("headers.timestamp");
		for (int i = 0; i<1000000; i++) {
			expression1.getValue(context, message, String.class);
			expression2.getValue(context, message, Long.class);
		}
	}

}
