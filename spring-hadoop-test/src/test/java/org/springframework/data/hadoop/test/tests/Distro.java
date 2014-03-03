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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.util.VersionInfo;
import org.springframework.util.StringUtils;

/**
 * A hadoop distro used to limit when certain tests are run.
 *
 * @author Janne Valkealahti
 *
 */
public enum Distro {

	/**
	 * Vanilla Apache Hadoop 1.2
	 */
	HADOOP12,

	/**
	 * Vanilla Apache Hadoop 2.2
	 */
	HADOOP22,

	/**
	 * Cloudera CDH5
	 */
	CDH5,

	/**
	 * Pivotal HD 1.0
	 */
	PHD1;

	public static Set<Distro> resolveDistros() {
		Set<Distro> distros = new HashSet<Distro>();
		String version = VersionInfo.getVersion();

		if (StringUtils.hasText(version)) {

			// add specific distro, if it wasn't
			// pivotal or cloudera, it must be
			// vanilla/hortonworks.
			if (version.contains("phd")) {
				distros.add(PHD1);
			} else if (version.contains("cdh")) {
				distros.add(CDH5);
			} else if (version.startsWith("2.2")) {
				distros.add(HADOOP22);
			} else if (version.startsWith("1.2")) {
				distros.add(HADOOP12);
			}
		}

		return distros;
	}

}
