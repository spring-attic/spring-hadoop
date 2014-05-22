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

	private final static String DEFAULT_PATH_DELIMITER = "/";

	private final String delimiter;

	/**
	 * Instantiates a new path combine method executor with
	 * default delimiter '/'.
	 */
	public PathCombiningMethodExecutor() {
		this(DEFAULT_PATH_DELIMITER);
	}

	/**
	 * Instantiates a new path combine method executor.
	 *
	 * @param delimiter the delimiter for paths
	 */
	public PathCombiningMethodExecutor(String delimiter) {
		this.delimiter = delimiter;
	}

	@Override
	public TypedValue execute(EvaluationContext context, Object target, Object... arguments) throws AccessException {
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < arguments.length; i++) {
			buf.append(arguments[i]);
			if (i+1 < arguments.length) {
				buf.append(delimiter);
			}
		}
		return new TypedValue(buf.toString());
	}

}
