/*
 * Copyright 2011-2012 the original author or authors.
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
 * Utility class to generate new time based path. The "pathFormat" is used to specify path format in the 
 * {@link java.util.Formatter} style.
 *  
 * For example, the return path will be "/user/hadoop/data/2012/2/22/17/20/10" 
 * if pathFormat is "/user/hadoop/data/%1$tY/%1$tm/%1$td/%1$tH/%1$tM/%1$tS"
 * 
 * @see java.util.Formatter
 * @author Jarred Li
 *
 */
public class PathUtils {

	/**
	 * get file time based path in the format of {@link java.util.Formatter}.
	 * 
	 * @param pathFormat Formatted path, the variable in the path will be 
	 * 					 replaced by {@link java.util.Date}. 
	 * 					 http://docs.oracle.com/javase/6/docs/api/java/util/Formatter.html#dt 
	 * @param appendUUID Whether append UUID to the generated path
	 * 
	 * @return Generated path 
	 */
	public static String format(String pathFormat, boolean appendUUID) {
		if (pathFormat == null || pathFormat.length() == 0) {
			return "";
		}
		pathFormat = pathFormat.replace('/', File.separatorChar);
		StringBuffer strBuffer = new StringBuffer();

		Formatter formatter = new Formatter(strBuffer, Locale.US);
		formatter.format(pathFormat, new Date());

		if (!pathFormat.endsWith(File.separator)) {
			strBuffer.append(File.separator);
		}

		if (appendUUID) {
			strBuffer.append(UUID.randomUUID());
			strBuffer.append(File.separator);
		}

		return strBuffer.toString();
	}


	/**
	 * override method without appending UUID to the generated path
	 * 
	 * @param pathFormat Formatted path, the variable in the path will be 
	 * 					 replaced by {@link java.util.Date}.
	 * @return Generated path without appending UUID
	 */
	public static String format(String pathFormat) {
		return format(pathFormat, false);
	}
}
