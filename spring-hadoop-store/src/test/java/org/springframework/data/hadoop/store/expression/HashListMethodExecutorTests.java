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
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.MethodExecutor;
import org.springframework.expression.spel.support.ReflectiveMethodResolver;
import org.springframework.util.StopWatch;

/**
 * Tests for {@link HashListMethodExecutor}.
 *
 * @author Janne Valkealahti
 *
 */
public class HashListMethodExecutorTests extends AbstractExpressionTests {

	@Test
	public void testCompilables() throws Exception {
		assertExpression(new TestMethodResolver(), "list('APP1',{{'1TO5','APP1','APP2','APP3','APP4','APP5'}})", "1TO5_list", true);
	}

	@Test
	public void testNonCompilables() throws Exception {
	}

	@Test
	public void testPerf() throws Exception {
		StopWatch sw = new StopWatch("testPerf");
		sw.start("customexecutor");
		List<Object> list = new ArrayList<Object>();
		List<String> list1 = new ArrayList<String>();
		list1.add("APPS");
		list1.add("APP1");
		list.add(list1);
		for (int i = 0; i<10000; i++) {
			HashListMethodExecutor.list("APP1", list);
		}
		sw.stop();
		System.out.println(sw.prettyPrint());
	}

	private static class TestMethodResolver extends ReflectiveMethodResolver {

		@Override
		public MethodExecutor resolve(EvaluationContext context, Object targetObject, String name,
				List<TypeDescriptor> argumentTypes) throws AccessException {
			return super.resolve(context, HashListMethodExecutor.class, name, argumentTypes);
		}

	}

}
