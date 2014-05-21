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

import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * Helper class to work with a spel expressions resolving values
 * from a {@link Message}.
 *
 * @author Janne Valkealahti
 *
 */
public class MessageExpressionMethods {

	private final StandardEvaluationContext context;

	/**
	 * Instantiates a new message expression methods.
	 */
	public MessageExpressionMethods() {
		context = new StandardEvaluationContext();
		context.addMethodResolver(new MessagePartitionKeyMethodResolver());
		context.addPropertyAccessor(new MessagePartitionKeyPropertyAccessor());
	}

	/**
	 * Gets the value.
	 *
	 * @param <T> the generic type
	 * @param expression the expression
	 * @param desiredResultType the desired result type
	 * @return the value
	 * @throws EvaluationException the evaluation exception
	 */
	public <T> T getValue(Expression expression, Message<?> message, Class<T> desiredResultType) throws EvaluationException {
		Assert.notNull(expression, "Expression cannot be null");
		return expression.getValue(context, message, desiredResultType);
	}

}
