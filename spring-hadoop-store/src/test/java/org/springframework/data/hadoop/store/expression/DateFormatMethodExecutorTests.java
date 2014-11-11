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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.MethodExecutor;
import org.springframework.expression.spel.support.ReflectiveMethodResolver;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Tests for {@link DateFormatMethodExecutor} internal functionality.
 *
 * @author Janne Valkealahti
 *
 */
public class DateFormatMethodExecutorTests extends AbstractExpressionTests {

	@Test
	public void testCompilables() throws Exception {
		Message<String> rootObject = MessageBuilder.withPayload("foo-bar").setHeader("foo", "bar").build();
		Long timestamp = rootObject.getHeaders().getTimestamp();
		String nowYYYYMMzero = new SimpleDateFormat("yyyy/MM").format(new Date(0));
		String nowYYYY = new SimpleDateFormat("yyyy").format(new Date(timestamp));

		Message<Object> wrappedRootObject = new MessageExpressionMethods.MessageWrappedMessage(rootObject);

		assertExpression(new TestMethodResolver(), wrappedRootObject, "dateFormat('yyyy')", nowYYYY, true);
		assertExpression(new TestMethodResolver(), rootObject, "dateFormat('yyyy/MM', 0)", nowYYYYMMzero, true);
		assertExpression(new TestMethodResolver(), rootObject, "dateFormat('yyyyMMdd','2000-10-20')", "20001020", true);
		assertExpression(new TestMethodResolver(), rootObject, "dateFormat('yyyyMMdd','20001020','yyyyMMdd')", "20001020", true);
	}

	private static class TestMethodResolver extends ReflectiveMethodResolver {

		@Override
		public MethodExecutor resolve(EvaluationContext context, Object targetObject, String name,
				List<TypeDescriptor> argumentTypes) throws AccessException {
			MethodExecutor resolve = super.resolve(context, DateFormatMethodExecutor.class, name, argumentTypes);
			if (resolve == null) {
				resolve = super.resolve(context, targetObject, name, argumentTypes);
			}
			return resolve;
		}

	}

}
