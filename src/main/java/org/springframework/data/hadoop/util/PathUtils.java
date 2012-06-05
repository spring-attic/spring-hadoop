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

	private String rootPath;

	private String pathFormat = "yyyy/MM/dd/HH/mm/ss";

	private boolean appendUUID;


	/**
	 * get file time based path in the format of {@link java.text.SimpleDateFormat}.
	 * 
	 * @return the file path appended with time.
	 */
	public String format() {
		if (rootPath == null || rootPath.length() == 0) {
			return "";
		}
		StringBuffer strBuffer = new StringBuffer();
		strBuffer.append(rootPath);
		if (!rootPath.endsWith(File.separator)) {
			strBuffer.append(File.separator);
		}

		Date date = new Date();
		SimpleDateFormat format = new SimpleDateFormat(pathFormat);
		strBuffer.append(format.format(date));
		strBuffer.append(File.separator);
		
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
