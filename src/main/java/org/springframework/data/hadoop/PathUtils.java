/*
 * Copyright 2011 the original author or authors.
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

package org.springframework.data.hadoop;

import java.util.Calendar;


/**
 * Utility class to generate new path based on give path. 
 * The patter is ${rootPath}/${Year}/${Month}/${Day}/${Hour}/${Minute}/{Second}. For example,"/user/hadoop/data/2012/2/22/17/20/10"
 * 
 * @author Jarred Li
 *
 */
public class PathUtils {

	private String rootPath;


	public String getTimeBasedPathFromRoot() {
		if (rootPath == null || rootPath.length() == 0) {
			return "";
		}
		StringBuffer strBuffer = new StringBuffer();
		if (!rootPath.startsWith("/")) {
			strBuffer.append("/");
		}

		strBuffer.append(rootPath);
		if (!rootPath.endsWith("/")) {
			strBuffer.append("/");
		}
		Calendar cal = Calendar.getInstance();
		strBuffer.append(cal.get(Calendar.YEAR));
		strBuffer.append("/");
		strBuffer.append(cal.get(Calendar.MONTH) + 1);
		strBuffer.append("/");
		strBuffer.append(cal.get(Calendar.DAY_OF_MONTH));
		strBuffer.append("/");
		strBuffer.append(cal.get(Calendar.HOUR_OF_DAY));
		strBuffer.append("/");
		strBuffer.append(cal.get(Calendar.MINUTE));
		strBuffer.append("/");
		strBuffer.append(cal.get(Calendar.SECOND));
		strBuffer.append("/");
		return strBuffer.toString();
	}


	public String getRootPath() {
		return rootPath;
	}


	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}
}
