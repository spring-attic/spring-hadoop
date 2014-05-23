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

import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.MethodExecutor;
import org.springframework.expression.TypedValue;

/**
 * A {@link MethodExecutor} using an first argument as an
 * Object to get its hashcode and second argument expected to
 * be an Integer to calculate a simple bucket name.
 *
 * <p>Spel expression "hash(region,2)" would create either key
 * "0_hash" or "1_hash".
 *
 * @author Janne Valkealahti
 *
 */
public class HashMethodExecutor implements MethodExecutor {

	@Override
	public TypedValue execute(EvaluationContext context, Object target, Object... arguments) throws AccessException {
		if (arguments[1] instanceof Integer) {
			try {
				Integer buckets = ((Integer)arguments[1]);
				return new TypedValue(arguments[0].hashCode() % buckets + "_hash");
			} catch (Exception e) {
				throw new AccessException("Error creating hash", e);
			}
		}
		throw new AccessException("Argument " + arguments[1] + " not an Integer");
	}

}
