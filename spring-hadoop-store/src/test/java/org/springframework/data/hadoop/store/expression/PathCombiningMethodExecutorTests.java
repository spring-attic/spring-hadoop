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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.data.hadoop.store.partition.MessagePartitionStrategy;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.MethodExecutor;
import org.springframework.expression.MethodResolver;
import org.springframework.expression.spel.support.ReflectiveMethodResolver;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Tests for {@link PathCombiningMethodExecutor}.
 *
 * @author Janne Valkealahti
 *
 */
public class PathCombiningMethodExecutorTests extends AbstractExpressionTests {

	@Test
	public void testCompilables() throws Exception {
		Message<String> rootObject = MessageBuilder.withPayload("foo-bar").setHeader("foo", "bar").build();
		Long timestamp = rootObject.getHeaders().getTimestamp();

		assertExpression(new TestMethodResolver(), "path('foo1')", "foo1", true);
		assertExpression(new TestMethodResolver(), "path('foo1','foo2')", "foo1/foo2", true);
		assertExpression(new TestMethodResolver(), "path(payload)", "foo-bar", true);
		assertExpression(new TestMethodResolver(), rootObject, "path(headers.timestamp)", timestamp.toString(), true);
	}

	@Test
	public void testNonCompilables() throws Exception {
		assertExpression(new TestMethodResolver(), "path(new String[]{'foo1'})", "foo1", false);
	}

	@Test
	public void testWithExtraResolver() {
		String expression = "path('dummy','partition')";
		StandardEvaluationContext context = new StandardEvaluationContext();
		MessagePartitionKeyMethodResolver resolver = new MessagePartitionKeyMethodResolver();
		MessagePartitionKeyPropertyAccessor accessor = new MessagePartitionKeyPropertyAccessor();
		context.addMethodResolver(resolver);
		context.addPropertyAccessor(accessor);
		MessagePartitionStrategy<String> strategy = new MessagePartitionStrategy<String>(expression, context);
		Message<String> message = MessageBuilder.withPayload("dummy").build();
		strategy.getPartitionResolver().resolvePath(message);
	}

	@Test
	public void testWithReplacedResolver() {
		String expression = "path('dummy','partition')";
		StandardEvaluationContext context = new StandardEvaluationContext();
		MessagePartitionKeyMethodResolver resolver = new MessagePartitionKeyMethodResolver();
		MessagePartitionKeyPropertyAccessor accessor = new MessagePartitionKeyPropertyAccessor();
		List<MethodResolver> methodResolvers = new ArrayList<MethodResolver>();
		methodResolvers.add(resolver);
		context.setMethodResolvers(methodResolvers);
		context.addPropertyAccessor(accessor);
		MessagePartitionStrategy<String> strategy = new MessagePartitionStrategy<String>(expression, context);
		Message<String> message = MessageBuilder.withPayload("dummy").build();
		strategy.getPartitionResolver().resolvePath(message);
	}

	private static class TestMethodResolver extends ReflectiveMethodResolver {

		@Override
		public MethodExecutor resolve(EvaluationContext context, Object targetObject, String name,
				List<TypeDescriptor> argumentTypes) throws AccessException {
			return super.resolve(context, PathCombiningMethodExecutor.class, name, argumentTypes);
		}

	}

}
