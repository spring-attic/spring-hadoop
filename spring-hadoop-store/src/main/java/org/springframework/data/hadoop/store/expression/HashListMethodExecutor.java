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

import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.MethodExecutor;
import org.springframework.expression.TypedValue;

/**
 * A {@link MethodExecutor} using an first argument as key
 * to be matched from second argument which is expected to be
 * a List of List of Objects.
 *
 * <p>Use case is to use spel expression
 * "list(region,{{'nordic','fin','swe'},{'britain','eng','sco'}})" to partition
 * value in 'region' so that resolved partition value is 'nordic' if any of a
 * 'nordic','fin', or 'swe' is matched. Resolved value will be suffixed with
 * '_list' and default value if there are no matches is 'list'.
 *
 * @author Janne Valkealahti
 *
 */
public class HashListMethodExecutor implements MethodExecutor {

	@Override
	public TypedValue execute(EvaluationContext context, Object target, Object... arguments) throws AccessException {
		if (arguments[1] instanceof List) {
			List<?> list = (List<?>)arguments[1];
			return new TypedValue(listWithObjects(arguments[0], list));
		} else {
			throw new AccessException("Argument " + arguments[1] + " not a List");
		}
	}

	public static String list(String arg1, List<Object> arg2) {
		return listWithObjects(arg1, arg2);
	}

	private static String listWithObjects(Object arg1, List<?> arg2) {
		for (Object object : arg2) {
			if (object instanceof List) {
				List<?> sublist = ((List<?>)object);
				if (sublist.contains(arg1)) {
					return sublist.get(0) + "_list";
				}
			}
		}
		return "list";
	}

}
