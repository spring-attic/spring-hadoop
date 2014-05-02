/*
 * Copyright 2011-2013 the original author or authors.
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

package org.springframework.data.hadoop.util;

import java.io.File;
import java.util.Date;
import java.util.Formatter;
import java.util.Locale;
import java.util.UUID;


/**
 * Utility for generating date-based paths.
 * Relies on the {@link java.util.Formatter} style.
 * <p>
 * For example, to generate the following path "/user/hadoop/data/2012/2/22/17/20/10"
 * the input format can be "/user/hadoop/data/%1$tY/%1$tm/%1$td/%1$tH/%1$tM/%1$tS"
 *
 * @see java.util.Formatter
 * @author Jarred Li
 */
public abstract class PathUtils {

	/**
	 * Generates a timed-based path, based on the current date, using the format of {@link java.util.Formatter}.
	 *
	 * @param pathFormat Formatted path, the variable in the path will be
	 * 					 replaced by {@link java.util.Date}.
	 * 					 http://docs.oracle.com/javase/6/docs/api/java/util/Formatter.html#dt
	 * @param appendUUID Whether a UUID is appended to the generated path
	 * @return generated path
	 */
	public static String format(String pathFormat, boolean appendUUID) {
		return format(pathFormat, appendUUID, new Date());
	}


	/**
	 * Generates a timed-based path, based on the current date,
	 * using the format of {@link java.util.Formatter}.
	 *
	 * @param pathFormat Path format, the variable in the path will be
	 * 					 replaced by {@link java.util.Date}.
	 * @return generated path
	 */
	public static String format(String pathFormat) {
		return format(pathFormat, false);
	}

	/**
	 * Generates a timed-based path, based on the given date, using the format of {@link java.util.Formatter}.
	 *
	 * @param pathFormat Path format, the variable in the path will be
	 * 					 replaced by {@link java.util.Date}.
	 * @param date date to use
	 * @return generated path
	 */
	public static String format(String pathFormat, Date date) {
		return format(pathFormat, false, date);
	}


	/**
	 * Generates a timed-based path, based on the given date, using the format of {@link java.util.Formatter}.
	 *
	 * @param pathFormat Path format, the variable in the path will be
	 * 					 replaced by {@link java.util.Date}.
	 * @param appendUUID whether or not to append a UUID at the end
	 * @param date date to use
	 * @return generated path
	 */
	public static String format(String pathFormat, boolean appendUUID, Date date) {
		if (pathFormat == null || pathFormat.length() == 0) {
			return "";
		}
		pathFormat = pathFormat.replace('/', File.separatorChar);
		StringBuilder strBuffer = new StringBuilder();

		Formatter formatter = new Formatter(strBuffer, Locale.US);
		formatter.format(pathFormat, date);
		formatter.close();

		if (!pathFormat.endsWith(File.separator)) {
			strBuffer.append(File.separator);
		}

		if (appendUUID) {
			strBuffer.append(UUID.randomUUID());
			strBuffer.append(File.separator);
		}

		return strBuffer.toString();
	}
}