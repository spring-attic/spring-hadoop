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
public class PartitionKeyMethodResolver implements MethodResolver {

	public final static String METHOD_DATEFORMAT = "dateformat";

	public final static String METHOD_PATH = "path";

	public final static String METHOD_HASH = "hash";

	public final static String METHOD_HASHLIST = "list";

	public final static String METHOD_HASHRANGE = "range";

	@Override
	public final MethodExecutor resolve(EvaluationContext context, Object targetObject, String name,
			List<TypeDescriptor> argumentTypes) throws AccessException {
		return dispatch(context, targetObject, name, argumentTypes);
	}

	/**
	 * Dispatch handling to an appropriate method supported by
	 * this {@link PartitionKeyMethodResolver}.
	 *
	 * @param context the current evaluation context
	 * @param targetObject the object upon which the method is being called
	 * @param argumentTypes the arguments that the constructor must be able to handle
	 * @return a MethodExecutor that can invoke the method, or null if the method cannot be found
	 * @throws AccessException the access exception
	 */
	protected MethodExecutor dispatch(EvaluationContext context, Object targetObject, String name,
			List<TypeDescriptor> argumentTypes) throws AccessException {
		String methodName = name != null ? name.toLowerCase() : null;
		if (METHOD_DATEFORMAT.equals(methodName)) {
			return doDateFormat(context, targetObject, methodName, argumentTypes);
		} else if (METHOD_PATH.equals(methodName)) {
			return doPath(context, targetObject, methodName, argumentTypes);
		} else if (METHOD_HASH.equals(methodName)) {
			return doHash(context, targetObject, methodName, argumentTypes);
		} else if (METHOD_HASHLIST.equals(methodName)) {
			return doHashList(context, targetObject, methodName, argumentTypes);
		} else if (METHOD_HASHRANGE.equals(methodName)) {
			return doHashRange(context, targetObject, methodName, argumentTypes);
		}
		// return null as method not found
		return null;
	}

	/**
	 * Create a {@link MethodExecutor} using {@link DateFormatMethodExecutor}.
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
			return new DateFormatMethodExecutor("timestamp");
		} else if (argumentTypes.size() == 2 || argumentTypes.size()==3) {
			return new DateFormatMethodExecutor(null);
		} else {
			throw new AccessException("Too many or missing arguments");
		}
	}

	/**
	 * Create a {@link MethodExecutor} using {@link HashMethodExecutor}.
	 *
	 * @param context the current evaluation context
	 * @param targetObject the object upon which the method is being called
	 * @param argumentTypes the arguments that the constructor must be able to handle
	 * @return a MessageDateFormatMethodExecutor
	 * @throws AccessException the access exception
	 */
	protected MethodExecutor doHash(EvaluationContext context, Object targetObject, String name,
			List<TypeDescriptor> argumentTypes) throws AccessException {
		return new HashMethodExecutor();
	}

	/**
	 * Create a {@link MethodExecutor} using {@link PathCombineMethodExecutor}.
	 *
	 * @param context the current evaluation context
	 * @param targetObject the object upon which the method is being called
	 * @param argumentTypes the arguments that the constructor must be able to handle
	 * @return a MessageDateFormatMethodExecutor
	 * @throws AccessException the access exception
	 */
	protected MethodExecutor doPath(EvaluationContext context, Object targetObject, String name,
			List<TypeDescriptor> argumentTypes) throws AccessException {
		return new PathCombineMethodExecutor();
	}

	/**
	 * Create a {@link MethodExecutor} using {@link HashListMethodExecutor}.
	 *
	 * @param context the current evaluation context
	 * @param targetObject the object upon which the method is being called
	 * @param argumentTypes the arguments that the constructor must be able to handle
	 * @return a MessageDateFormatMethodExecutor
	 * @throws AccessException the access exception
	 */
	protected MethodExecutor doHashList(EvaluationContext context, Object targetObject, String name,
			List<TypeDescriptor> argumentTypes) throws AccessException {
		return new HashListMethodExecutor();
	}

	/**
	 * Create a {@link MethodExecutor} using {@link HashRangeMethodExecutor}.
	 *
	 * @param context the current evaluation context
	 * @param targetObject the object upon which the method is being called
	 * @param argumentTypes the arguments that the constructor must be able to handle
	 * @return a MessageDateFormatMethodExecutor
	 * @throws AccessException the access exception
	 */
	protected MethodExecutor doHashRange(EvaluationContext context, Object targetObject, String name,
			List<TypeDescriptor> argumentTypes) throws AccessException {
		return new HashRangeMethodExecutor();
	}

}
