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

import java.util.List;

import org.springframework.core.convert.TypeDescriptor;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.MethodExecutor;
import org.springframework.expression.MethodResolver;

/**
 * A {@link Message} specific {@link MethodResolver}.
 *
 * @author Janne Valkealahti
 *
 */
public class MessagePartitionKeyMethodResolver extends PartitionKeyMethodResolver {

	/**
	 * Create a {@link MethodExecutor} using {@link MessageDateFormatMethodExecutor}.
	 *
	 * @param context the current evaluation context
	 * @param targetObject the object upon which the method is being called
	 * @param argumentTypes the arguments that the constructor must be able to handle
	 * @return a MessageDateFormatMethodExecutor
	 * @throws AccessException the access exception
	 */
	protected MethodExecutor doDateFormat(EvaluationContext context, Object targetObject, String name,
			List<TypeDescriptor> argumentTypes) throws AccessException {
		if (argumentTypes.size() == 1) {
			return new MessageDateFormatMethodExecutor("timestamp");
		} else if (argumentTypes.size() == 2 || argumentTypes.size()==3) {
			return new MessageDateFormatMethodExecutor();
		} else {
			throw new AccessException("Too many or missing arguments");
		}
	}

}
