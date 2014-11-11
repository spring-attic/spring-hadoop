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

import java.util.Arrays;
import java.util.List;

import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.MethodExecutor;
import org.springframework.expression.TypedValue;

/**
 * A {@link MethodExecutor} using an first argument as key
 * to be used as a range search with a List of Object found from
 * a second argument.
 *
 * <p> Spel expression for this would be "range(region,{10,20,30,40})".
 * Value 15 would create key "20_range", 40 key "40_range" and
 * 45 key "40_range".
 *
 * @author Janne Valkealahti
 *
 */
public class HashRangeMethodExecutor implements MethodExecutor {

	@Override
	public TypedValue execute(EvaluationContext context, Object target, Object... arguments) throws AccessException {
		if (arguments[1] instanceof List) {
			return new TypedValue(rangeWithObjectAndList(arguments[0], (List<?>) arguments[1]));
		}
		throw new AccessException("Argument " + arguments[1] + " not a List");
	}

	public static String range(Object arg1, List<?> arg2) throws AccessException {
		return rangeWithObjectAndList(arg1, arg2);
	}

	private static String rangeWithObjectAndList(Object arg1, List<?> arg2) throws AccessException {
		try {
			Object[] ranges = ((List<?>) arg2).toArray(new Object[0]);
			int searchIndex = Arrays.binarySearch(ranges, arg1);
			return ranges[Math.min(searchIndex < 0 ? -searchIndex - 1 : searchIndex, ranges.length - 1)] + "_range";
		} catch (Exception e) {
			throw new AccessException("Error finding range", e);
		}
	}

}
