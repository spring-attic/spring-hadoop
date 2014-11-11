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
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.MethodExecutor;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.support.ReflectiveMethodResolver;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * Tests for {@link HashMethodExecutor} internal functionality.
 *
 * @author Janne Valkealahti
 *
 */
public class HashMethodExecutorTests extends AbstractExpressionTests {

	@Test
	public void testPositiveValues() throws Exception {
		StandardEvaluationContext context = new StandardEvaluationContext();
		HashMethodExecutor executor = new HashMethodExecutor();
		TypedValue value = executor.execute(context, new Object(), 3, 2);
		assertThat((String) value.getValue(), is("1_hash"));
		value = executor.execute(context, new Object(), 4, 2);
		assertThat((String) value.getValue(), is("0_hash"));
		value = executor.execute(context, new Object(), 9, 27);
		assertThat((String) value.getValue(), is("9_hash"));
		value = executor.execute(context, new Object(), 9, 11);
		assertThat((String) value.getValue(), is("9_hash"));
		value = executor.execute(context, new Object(), 11, 22);
		assertThat((String) value.getValue(), is("11_hash"));
		value = executor.execute(context, new Object(), 30, 27);
		assertThat((String) value.getValue(), is("3_hash"));
		value = executor.execute(context, new Object(), 332, 27);
		assertThat((String) value.getValue(), is("8_hash"));
	}

	@Test
	public void testNegativeValues() throws Exception {
		StandardEvaluationContext context = new StandardEvaluationContext();
		HashMethodExecutor executor = new HashMethodExecutor();
		TypedValue value = executor.execute(context, new Object(), -3, -2);
		assertThat((String) value.getValue(), is("1_hash"));
	}

	@Test
	public void testEqualValues() throws Exception {
		StandardEvaluationContext context = new StandardEvaluationContext();
		HashMethodExecutor executor = new HashMethodExecutor();
		TypedValue value = executor.execute(context, new Object(), -3, -3);
		assertThat((String) value.getValue(), is("0_hash"));
		value = executor.execute(context, new Object(), -1, -1);
		assertThat((String) value.getValue(), is("0_hash"));
		value = executor.execute(context, new Object(), 1, 1);
		assertThat((String) value.getValue(), is("0_hash"));
	}

	@Test(expected=AccessException.class)
	public void testZeros() throws Exception {
		StandardEvaluationContext context = new StandardEvaluationContext();
		HashMethodExecutor executor = new HashMethodExecutor();
		executor.execute(context, new Object(), 0, 0);
	}

	@Test
	public void testNegativeBucketSize() throws Exception {
		StandardEvaluationContext context = new StandardEvaluationContext();
		HashMethodExecutor executor = new HashMethodExecutor();
		TypedValue value = executor.execute(context, new Object(), 3, -2);
		assertThat((String) value.getValue(), is("1_hash"));
	}

	@Test
	public void testNegativeHashcode() throws Exception {
		StandardEvaluationContext context = new StandardEvaluationContext();
		HashMethodExecutor executor = new HashMethodExecutor();
		TypedValue value = executor.execute(context, new Object(), -9, 2);
		assertThat((String) value.getValue(), is("1_hash"));
	}

	@Test
	public void testNegativeHashcodeBucketSizeBigger() throws Exception {
		StandardEvaluationContext context = new StandardEvaluationContext();
		HashMethodExecutor executor = new HashMethodExecutor();
		TypedValue value = executor.execute(context, new Object(), -9, 12);
		assertThat((String) value.getValue(), is("9_hash"));
		value = executor.execute(context, new Object(), -9, 24);
		assertThat((String) value.getValue(), is("9_hash"));
		value = executor.execute(context, new Object(), -9, 27);
		assertThat((String) value.getValue(), is("9_hash"));
	}

	@Test
	public void testCompilables() throws Exception {
		assertExpression(new TestMethodResolver(), "hash(3,2)", "1_hash", true);
	}

//	@Test
//	public void testNonCompilables() throws Exception {
//		assertExpression(new TestMethodResolver(), "path(new String[]{'foo1'})", "foo1", false);
//	}

	private static class TestMethodResolver extends ReflectiveMethodResolver {

		@Override
		public MethodExecutor resolve(EvaluationContext context, Object targetObject, String name,
				List<TypeDescriptor> argumentTypes) throws AccessException {
			return super.resolve(context, HashMethodExecutor.class, name, argumentTypes);
		}

	}

}
