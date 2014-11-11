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
 * A {@link MethodExecutor} making it easier to combine
 * paths instead of concatenating in spel itself.
 *
 * @author Janne Valkealahti
 *
 */
public class PathCombiningMethodExecutor implements MethodExecutor {

	private final static String PATH_DELIMITER = "/";

	public PathCombiningMethodExecutor() {
	}

	@Override
	public TypedValue execute(EvaluationContext context, Object target, Object... arguments) throws AccessException {
		return new TypedValue(pathWithObjectArray(arguments));
	}

	public static String path(String arg) throws AccessException {
		return pathWithObjectArray(new String[]{arg});
	}

	public static String path(String... arguments) throws AccessException {
		return pathWithObjectArray(arguments);
	}

	public static String path(Long arg) throws AccessException {
		return pathWithObjectArray(new Long[]{arg});
	}

	public static String path(Long... arguments) throws AccessException {
		return pathWithObjectArray(arguments);
	}

	public static String path(Object arg) throws AccessException {
		return pathWithObjectArray(new Object[]{arg});
	}

	public static String path(Object... arguments) throws AccessException {
		return pathWithObjectArray(arguments);
	}

	private static String pathWithObjectArray(Object[] arguments) throws AccessException {
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < arguments.length; i++) {
			buf.append(arguments[i]);
			if (i+1 < arguments.length) {
				buf.append(PATH_DELIMITER);
			}
		}
		return buf.toString();
	}

}
