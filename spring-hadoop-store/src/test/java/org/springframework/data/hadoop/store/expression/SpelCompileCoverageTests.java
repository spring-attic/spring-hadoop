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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.MethodResolver;
import org.springframework.expression.spel.SpelCompilerMode;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelCompiler;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Various coverage tests for spel compiled expressions.
 *
 * @author Janne Valkealahti
 *
 */
public class SpelCompileCoverageTests {

	@Test
	public void testCompilables() throws Exception {
		Message<String> rootObject = MessageBuilder.withPayload("jee-juu").setHeader("region", "us").build();
		Message<Object> wrappedRootObject = new MessageExpressionMethods.MessageWrappedMessage(rootObject);
		Long timestamp = rootObject.getHeaders().getTimestamp();

		assertExpression("path('dummy','partition')", "dummy/partition", true);
		assertExpression("path('dummy',range(1,{3,5,10}))", "dummy/3_range", true);
		assertExpression("'dummy'", "dummy", true);
		assertExpression("1 + 1", "2", true);
		assertExpression("'dummy' + '/' + 'partition'", "dummy/partition", true);
		assertExpression("'dummy' + 'partition'", "dummypartition", true);
		assertExpression("'dummy/' + range(1,{3,5,10})", "dummy/3_range", true);

		assertExpression("payload.split('-')[0]", "jee", true);
		assertExpression("path(payload.split('-')[0])", "jee", true);

		String nowYYYYMMdd = new SimpleDateFormat("yyyy/MM/dd").format(new Date());
		String expression = "path(dateFormat('yyyy/MM/dd'),list(payload.split('-')[0],{{'JEE','jee'},{'JUU','juu'}}))";
		assertExpression(wrappedRootObject, expression, nowYYYYMMdd+"/JEE_list", true);

		expression = "path(dateFormat('yyyy/MM/dd'),list(payload.split('-')[0],{{'1TO5','APP1','APP2','APP3','APP4','jee'},{'6TO10','APP6','APP7','APP8','APP9','APP10'}}))";
		assertExpression(wrappedRootObject, expression, nowYYYYMMdd+"/1TO5_list", true);

		assertExpression("path(T(org.springframework.util.StringUtils).split(payload,'-')[0])", "jee", true);
		assertExpression("list('APP1',{{'1TO5','APP1'}})", "1TO5_list", true);

		assertExpression(wrappedRootObject, "region", "us", true);
		assertExpression(wrappedRootObject, "headers[region]", "us", true);

		String nowYYYY = new SimpleDateFormat("yyyy").format(new Date(timestamp));
		assertExpression(wrappedRootObject, "headers[region].toString() + '/' + dateFormat('yyyy', headers[timestamp])", "us/"
				+ nowYYYY, true);
	}

	@Test
	public void testNonCompilables() throws Exception {
	}

	private static void assertExpression(String expression, String result, boolean compilable) throws Exception {
		Message<String> message = MessageBuilder.withPayload("jee-juu").build();
		assertExpression(message, expression, result, compilable);
	}

	private static void assertExpression(Object rootObject, String expression, String result, boolean compilable) throws Exception {
		SpelParserConfiguration spelParserConfiguration = new SpelParserConfiguration(SpelCompilerMode.MIXED, null);
		ExpressionParser parser = new SpelExpressionParser(spelParserConfiguration);
		StandardEvaluationContext context = new StandardEvaluationContext();

		MessagePartitionKeyMethodResolver resolver = new MessagePartitionKeyMethodResolver();
		List<MethodResolver> methodResolvers = new ArrayList<MethodResolver>();
		methodResolvers.add(resolver);
		context.setMethodResolvers(methodResolvers);

		context.addPropertyAccessor(new MessagePartitionKeyPropertyAccessor());

		Expression exp = parser.parseExpression(expression);
		// looks like we need to call three times before
		// expression is compiled and we can check it
		assertThat(exp.getValue(context, rootObject, String.class), is(result));
		assertThat(exp.getValue(context, rootObject, String.class), is(result));
		assertThat(exp.getValue(context, rootObject, String.class), is(result));
		if (compilable) {
			assertCanCompile(exp);
		} else {
			assertCantCompile(exp);
		}
	}

	private static void assertCanCompile(Expression expression) {
		assertTrue("Expression \"" + expression.getExpressionString() + "\" not compilable", SpelCompiler.compile(expression));
	}

	private static void assertCantCompile(Expression expression) {
		assertFalse("Expression \"" + expression.getExpressionString() + "\" is compilable", SpelCompiler.compile(expression));
	}

}
