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
import java.util.ArrayList;
import java.util.List;

import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.MethodResolver;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;

/**
 * Helper class to work with a spel expressions resolving values
 * from a {@link Message}. A {@link StandardEvaluationContext} is expected and
 * {@link MessagePartitionKeyMethodResolver} and {@link MessagePartitionKeyPropertyAccessor}
 * registered with it.
 *
 * @author Janne Valkealahti
 *
 */
public class MessageExpressionMethods {

	private final StandardEvaluationContext context;

	/**
	 * Instantiates a new message expression methods with
	 * a {@link StandardEvaluationContext}.
	 *
	 * @param evaluationContext the spel evaluation context
	 */
	public MessageExpressionMethods(StandardEvaluationContext evaluationContext) {
		this(evaluationContext, false, false);
	}

	/**
	 * Instantiates a new message expression methods with
	 * a {@link StandardEvaluationContext}.
	 *
	 * @param evaluationContext the spel evaluation context
	 * @param autoCustomize auto customize method resolver and property accessor
	 * @param replaceMethodResolver replace context method resolver
	 */
	public MessageExpressionMethods(StandardEvaluationContext evaluationContext, boolean autoCustomize, boolean replaceMethodResolver) {
		Assert.notNull(evaluationContext, "Evaluation context cannot be null");
		if (autoCustomize) {
			MethodResolver methodResolver = new MessagePartitionKeyMethodResolver();
			if (replaceMethodResolver) {
				List<MethodResolver> methodResolvers = new ArrayList<MethodResolver>();
				methodResolvers.add(methodResolver);
				evaluationContext.setMethodResolvers(methodResolvers);
			} else {
				evaluationContext.addMethodResolver(methodResolver);
			}
		}
		if (autoCustomize) {
			evaluationContext.addPropertyAccessor(new MessagePartitionKeyPropertyAccessor());
		}
		this.context = evaluationContext;
	}

	/**
	 * Gets the value.
	 *
	 * @param <T> the generic type
	 * @param expression the expression
	 * @param message the message
	 * @param desiredResultType the desired result type
	 * @return the value
	 * @throws EvaluationException the evaluation exception
	 */
	public <T> T getValue(Expression expression, Message<?> message, Class<T> desiredResultType)
			throws EvaluationException {
		Assert.notNull(expression, "Expression cannot be null");
		return expression.getValue(context, new MessageWrappedMessage(message), desiredResultType);
	}

	public static class MessageWrappedMessage implements Message<Object> {

		private final Message<?> delegate;

		public MessageWrappedMessage(Message<?> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Object getPayload() {
			return delegate.getPayload();
		}

		@Override
		public MessageHeaders getHeaders() {
			return delegate.getHeaders();
		}

		public String dateFormat(String pattern) {
			SimpleDateFormat format = new SimpleDateFormat(pattern);
			return format.format(getHeaders().getTimestamp());
		}

	}

}
