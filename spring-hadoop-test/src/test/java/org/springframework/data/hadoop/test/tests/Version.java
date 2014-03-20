/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.hadoop.test.tests;

import org.apache.hadoop.util.VersionInfo;
import org.springframework.util.StringUtils;

/**
 * A hadoop version used to limit when certain tests are run.
 *
 * @author Janne Valkealahti
 *
 */
public enum Version {

	/**
	 * All hadoop 1.x and MR1 based distros.
	 */
	HADOOP1X,

	/**
	 * All hadoop 2.x YARN based distros.
	 */
	HADOOP2X;

	public static Version resolveVersion() {
		String version = VersionInfo.getVersion();

		if (StringUtils.hasText(version)) {
			// 1.x or 2.x
			if (version.startsWith("1") || version.contains("mr1")) {
				return HADOOP1X;
			} else if (version.startsWith("2")) {
				return HADOOP2X;
			}

		}
		// should not get here
		return null;
	}

}
