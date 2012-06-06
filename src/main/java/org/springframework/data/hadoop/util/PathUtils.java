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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;


/**
 * Utility class to generate new time based path. The "pathFormat" is used to specify path format in the 
 * {@link java.text.SimpleDateFormat} style.
 *  
 * For example, the return path will be "/user/hadoop/data/2012/2/22/17/20/10" 
 * if pathFormat is "yyyy/MM/dd/HH/mm/ss" and rootPath is "/user/hadoop/data".
 * 
 * @author Jarred Li
 *
 */
public class PathUtils {

	/**
	 * get file time based path in the format of {@link java.text.SimpleDateFormat}.
	 * 
	 * @param rootPath Root path 
	 * @param pathFormat Path format to be generated
	 * @param appendUUID Whether append UUID to the generated path
	 * 
	 * @return Generated path "${rootPath}/${formattedPath}/${UUID}
	 */
	public static String format(String rootPath, String pathFormat, boolean appendUUID) {
		if (rootPath == null || rootPath.length() == 0) {
			return "";
		}
		if (pathFormat == null || pathFormat.length() == 0) {
			return "";
		}
		pathFormat = pathFormat.replace('/', File.separatorChar);
		StringBuffer strBuffer = new StringBuffer();

		strBuffer.append(rootPath);
		if (!rootPath.endsWith(File.separator)) {
			strBuffer.append(File.separator);
		}
		
		Date date = new Date();
		SimpleDateFormat format = new SimpleDateFormat(pathFormat);
		strBuffer.append(format.format(date));

		if (!pathFormat.endsWith("/")) {
			strBuffer.append(File.separator);
		}

		if (appendUUID) {
			strBuffer.append(UUID.randomUUID());
			strBuffer.append(File.separator);
		}

		return strBuffer.toString();
	}

}

