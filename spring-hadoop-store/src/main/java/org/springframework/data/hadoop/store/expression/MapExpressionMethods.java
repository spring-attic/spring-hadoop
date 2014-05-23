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

import java.util.Map;

import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

/**
 * Helper class to work with a spel expressions resolving values
 * from a {@link Map}.
 *
 * @author Janne Valkealahti
 *
 */
public class MapExpressionMethods {

	private final StandardEvaluationContext context;

	/**
	 * Instantiates a new map expression methods.
	 */
	public MapExpressionMethods() {
		this(null);
	}

	/**
	 * Instantiates a new message expression methods with
	 * a {@link EvaluationContext} which is expected to be
	 * a {@link StandardEvaluationContext}.
	 */
	public MapExpressionMethods(EvaluationContext evaluationContext) {
		if (evaluationContext instanceof StandardEvaluationContext) {
			// we were given a context, so don't register anything
			context = (StandardEvaluationContext) evaluationContext;
		} else {
			context = new StandardEvaluationContext();
			context.addMethodResolver(new PartitionKeyMethodResolver());
			context.addPropertyAccessor(new MapAccessor());
		}
	}

	/**
	 * Gets the value.
	 *
	 * @param <T> the generic return type
	 * @param expression the expression
	 * @param desiredResultType the desired result type
	 * @return the value of expression evaluation
	 * @throws EvaluationException the evaluation exception
	 */
	public <T> T getValue(Expression expression, Map<String,Object> message, Class<T> desiredResultType) throws EvaluationException {
		Assert.notNull(expression, "Expression cannot be null");
		return expression.getValue(context, message, desiredResultType);
	}

}
