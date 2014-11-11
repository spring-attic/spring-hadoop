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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.MethodExecutor;
import org.springframework.expression.TypedValue;

/**
 * A {@link MethodExecutor} handling formatting using a {@link SimpleDateFormat}.
 *
 * @author Janne Valkealahti
 * @author Rodrigo Meneses
 *
 */
public class DateFormatMethodExecutor implements MethodExecutor {

	private final static String DEFAULT_FORMAT = "yyyy-MM-dd";

	private String key;

	/**
	 * Instantiates a new date format method executor.
	 */
	public DateFormatMethodExecutor() {
	}

	/**
	 * Instantiates a new date format method executor.
	 *
	 * @param key the key for timestamp
	 */
	public DateFormatMethodExecutor(String key) {
		this.key = key;
	}

	@Override
	public TypedValue execute(EvaluationContext context, Object target, Object... arguments) throws AccessException {
		if (key == null) {
			SimpleDateFormat format = new SimpleDateFormat((String)arguments[0]);
			//if first argument is Long, then we assume it's a timestamp in milliseconds type format
			//first argument can also be a Date itself
			if (arguments[1] instanceof Long || arguments[1] instanceof Date) {
				return new TypedValue(format.format(arguments[1]));
			}
			//if the argument is a String, then assume it's a Date represented as a String.
			if (arguments[1] instanceof String) {
				//Assume it's in default formay yyyMMdd
				SimpleDateFormat fromFormat = new SimpleDateFormat(DEFAULT_FORMAT);
				//if the third argument is present, use it as the from date format
				if (arguments.length == 3 && arguments[2] instanceof String)
					fromFormat = new SimpleDateFormat((String)arguments[2]);

					try {
						Date parsedDate = fromFormat.parse((String) arguments[1]);
						return new TypedValue(format.format(parsedDate));
					} catch (ParseException e) {
						throw new AccessException("Unable to format", e);
					}

			}

			return new TypedValue(format.format((Long)arguments[1]));
		}
		throw new AccessException("Unable to format");
	}

	/**
	 * Gets the key for timestamp if defined.
	 *
	 * @return the timestamp key
	 */
	protected String getKey() {
		return key;
	}

	public static String dateFormat(String pattern, Integer epoch) throws AccessException {
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		return format.format(epoch);
	}

	public static String dateFormat(String pattern, Long epoch) throws AccessException {
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		return format.format(epoch);
	}

	public static String dateFormat(String pattern, Date date) throws AccessException {
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		return format.format(date);
	}

	public static String dateFormat(String pattern, String datestring) throws AccessException {
		try {
			SimpleDateFormat format = new SimpleDateFormat(pattern);
			SimpleDateFormat fromFormat = new SimpleDateFormat(DEFAULT_FORMAT);
			Date parsedDate = fromFormat.parse(datestring);
			return format.format(parsedDate);
		} catch (ParseException e) {
			throw new AccessException("Unable to format", e);
		}
	}

	public static String dateFormat(String pattern, String datestring, String dateformat) throws AccessException {
		try {
			SimpleDateFormat format = new SimpleDateFormat(pattern);
			SimpleDateFormat fromFormat = new SimpleDateFormat(dateformat);
			Date parsedDate = fromFormat.parse(datestring);
			return format.format(parsedDate);
		} catch (ParseException e) {
			throw new AccessException("Unable to format", e);
		}
	}

}
