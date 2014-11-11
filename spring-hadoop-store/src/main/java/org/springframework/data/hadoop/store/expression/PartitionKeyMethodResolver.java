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
import org.springframework.expression.spel.support.ReflectiveMethodResolver;

/**
 * A {@link MethodResolver} handling custom methods internally without
 * a need to register via variables.
 *
 * @author Janne Valkealahti
 * @author Rodrigo Meneses
 */
public class PartitionKeyMethodResolver extends ReflectiveMethodResolver {

	public final static String METHOD_DATEFORMAT = "dateFormat";

	public final static String METHOD_PATH = "path";

	public final static String METHOD_HASH = "hash";

	public final static String METHOD_HASHLIST = "list";

	public final static String METHOD_HASHRANGE = "range";

	public PartitionKeyMethodResolver() {
		super();
	}

	public PartitionKeyMethodResolver(boolean useDistance) {
		super(useDistance);
	}

	@Override
	public MethodExecutor resolve(EvaluationContext context, Object targetObject, String name,
			List<TypeDescriptor> argumentTypes) throws AccessException {
		// first check against given targetObject
		MethodExecutor executor = super.resolve(context, targetObject, name, argumentTypes);
		if (executor != null) {
			return executor;
		}

		// intercept our own function names and replace target so
		// that we can match a correct partition functions.
		if (METHOD_PATH.equals(name)) {
			targetObject = PathCombiningMethodExecutor.class;
		} else if (METHOD_DATEFORMAT.equals(name)) {
			targetObject = DateFormatMethodExecutor.class;
		} else if (METHOD_HASH.equals(name)) {
			targetObject = HashMethodExecutor.class;
		} else if (METHOD_HASHLIST.equals(name)) {
			targetObject = HashListMethodExecutor.class;
		} else if (METHOD_HASHRANGE.equals(name)) {
			targetObject = HashRangeMethodExecutor.class;
		}
		// need to go back to super method for whole spel to work
		return super.resolve(context, targetObject, name, argumentTypes);
	}

}
