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
import java.util.Calendar;
import java.util.UUID;


/**
 * Utility class to generate new path based on give path. 
 * The pattern is like ${rootPath}/${Year}/${Month}/${Day}/${Hour}/${Minute}/{Second} based on pathFormat. 
 * For example, the return path will be "/user/hadoop/data/2012/2/22/17/20/10" if pathFormat is "year/month/day/hour/minute/second"
 * 
 * @author Jarred Li
 *
 */
public class PathUtils {

	private String rootPath;

	private String pathFormat = "year/month/day/hour/minute/second";

	private boolean appendUUID;


	/**
	 * get file path based on time. For example "/user/hadoop/data/2012/2/22/17/20/10"
	 * 
	 * @return the file path appended with time.
	 */
	public String getTimeBasedPathFromRoot() {
		if (rootPath == null || rootPath.length() == 0) {
			return "";
		}
		StringBuffer strBuffer = new StringBuffer();
		strBuffer.append(rootPath);
		if (!rootPath.endsWith(File.separator)) {
			strBuffer.append(File.separator);
		}

		Calendar cal = Calendar.getInstance();
		int year = cal.get(Calendar.YEAR);
		int month = cal.get(Calendar.MONTH) + 1;
		int day = cal.get(Calendar.DAY_OF_MONTH);
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		int minute = cal.get(Calendar.MINUTE);
		int second = cal.get(Calendar.SECOND);

		String[] formats = this.pathFormat.split("/");
		for (String format : formats) {
			switch (PathSeparator.valueOf(format.toLowerCase())) {
			case year:
				strBuffer.append(year);
				strBuffer.append(File.separator);
				break;
			case month:
				strBuffer.append(month);
				strBuffer.append(File.separator);
				break;
			case day:
				strBuffer.append(day);
				strBuffer.append(File.separator);
				break;
			case hour:
				strBuffer.append(hour);
				strBuffer.append(File.separator);
				break;
			case minute:
				strBuffer.append(minute);
				strBuffer.append(File.separator);
				break;
			case second:
				strBuffer.append(second);
				strBuffer.append(File.separator);
				break;
			default:
				break;
			}
		}
		if (this.appendUUID) {
			strBuffer.append(UUID.randomUUID());
			strBuffer.append(File.separator);
		}

		return strBuffer.toString();
	}


	/**
	 * get root path
	 * @return root path
	 */
	public String getRootPath() {
		return rootPath;
	}


	/**
	 * set root path
	 * @param rootPath root path name
	 */
	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}


	/**
	 * @return the pathFormat
	 */
	public String getPathFormat() {
		return pathFormat;
	}


	/**
	 * @param pathFormat the pathFormat to set
	 */
	public void setPathFormat(String pathFormat) {
		this.pathFormat = pathFormat;
	}


	/**
	 * @return the appendUUID
	 */
	public boolean isAppendUUID() {
		return appendUUID;
	}


	/**
	 * @param appendUUID the appendUUID to set
	 */
	public void setAppendUUID(boolean appendUUID) {
		this.appendUUID = appendUUID;
	}
}

enum PathSeparator {
	year, month, day, hour, minute, second
}
