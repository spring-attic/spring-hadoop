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
 * A {@link MethodResolver} handling custom methods internally without
 * a need to register via variables.
 *
 * @author Janne Valkealahti
 * @author Rodrigo Meneses
 */
public class MessagePartitionKeyMethodResolver implements MethodResolver {

	public final static String METHOD_DATEFORMAT = "dateformat";

	public final static String METHOD_PATH = "path";

	@Override
	public MethodExecutor resolve(EvaluationContext context, Object targetObject, String name,
			List<TypeDescriptor> argumentTypes) throws AccessException {
		if (METHOD_DATEFORMAT.equals(name.toLowerCase())) {
			// arg1 should be a format pattern
			// arg2 if exists should be a timestamp
			if (argumentTypes.size() == 1) {
				return new DateFormatMethodExecutor("timestamp");
			} else if (argumentTypes.size() == 2 || argumentTypes.size()==3) {
				return new DateFormatMethodExecutor(null);
			} else {
				throw new AccessException("Too many or missing arguments");
			}
		} else if (METHOD_PATH.equals(name.toLowerCase())) {
			return new PathCombineMethodExecutor();
		}
		return null;
	}

}
