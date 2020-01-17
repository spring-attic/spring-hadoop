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

import java.util.ArrayList;
import java.util.List;

import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.MethodExecutor;
import org.springframework.expression.MethodResolver;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.spel.SpelCompilerMode;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelCompiler;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Base utilities for {@link MethodExecutor} tests.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractExpressionTests {

	protected static void assertExpression(MethodResolver resolver, String expression, String result, boolean compilable) throws Exception {
		assertExpression(resolver, null, false, null, expression, result, compilable);
	}

	protected static void assertExpression(MethodResolver resolver, Object rootObject, String expression, String result, boolean compilable) throws Exception {
		assertExpression(resolver, null, false, rootObject, expression, result, compilable);
	}

	protected static void assertExpression(PropertyAccessor accessor, String expression, String result, boolean compilable) throws Exception {
		assertExpression(null, accessor, false, null, expression, result, compilable);
	}

	protected static void assertExpression(PropertyAccessor accessor, Object rootObject, String expression, String result, boolean compilable) throws Exception {
		assertExpression(null, accessor, false, rootObject, expression, result, compilable);
	}

	protected static void assertExpression(MethodResolver resolver, PropertyAccessor accessor, boolean replaceAccessor, Object rootObject, String expression, String result, boolean compilable) throws Exception {
		SpelParserConfiguration spelParserConfiguration = new SpelParserConfiguration(SpelCompilerMode.MIXED, null);
		ExpressionParser parser = new SpelExpressionParser(spelParserConfiguration);
		StandardEvaluationContext context = new StandardEvaluationContext();

		if (resolver != null) {
			List<MethodResolver> methodResolvers = new ArrayList<MethodResolver>();
			methodResolvers.add(resolver);
			context.setMethodResolvers(methodResolvers);
		}

		if (accessor != null) {
			if (replaceAccessor) {
				List<PropertyAccessor> propertyAccessors = new ArrayList<PropertyAccessor>();
				propertyAccessors.add(accessor);
				context.setPropertyAccessors(propertyAccessors);
			} else {
				context.addPropertyAccessor(accessor);
			}
		}

		if (rootObject == null) {
			rootObject = MessageBuilder.withPayload("foo-bar").build();
		}
		Expression exp = parser.parseExpression(expression);
		assertCantCompile(exp);
		// looks like we need to call three times before
		// expression is compiled and we can check it
		assertThat(exp.getValue(context, rootObject, String.class), is(result));
		assertThat(exp.getValue(context, rootObject, String.class), is(result));
		assertThat(exp.getValue(context, rootObject, String.class), is(result));
		assertThat(exp.getValue(context, rootObject, String.class), is(result));
		if (compilable) {
			assertCanCompile(exp);
		} else {
			assertCantCompile(exp);
		}
	}

	protected static void assertCanCompile(Expression expression) {
		assertTrue("Expression \"" + expression.getExpressionString() + "\" not compilable", SpelCompiler.compile(expression));
	}

	protected static void assertCantCompile(Expression expression) {
		assertFalse("Expression \"" + expression.getExpressionString() + "\" is compilable", SpelCompiler.compile(expression));
	}

}
