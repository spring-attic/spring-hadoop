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
package org.springframework.yarn.test.support;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.StringUtils;
import org.springframework.yarn.test.context.YarnCluster;

/**
 * Utilities for checking container logs.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class ContainerLogUtils {

	static Log log = LogFactory.getLog(ContainerLogUtils.class);

	/**
	 * Find container logs for running cluster and application.
	 * <p>
	 * Created pattern which is used with {@code PathMatchingResourcePatternResolver}
	 * is resolved from {@code YarnCluster}, {@code ApplicationId} and fileName.
	 * For example if fileName is given as <code>*.std*</code>, pattern will look like
	 * <code>file:/path/to/project/target/yarn--1502101888/*logDir*&#47;application_1382082435804_0001&#47;**&#47;*.std*</code>
	 *
	 * @param yarnCluster the yarn cluster
	 * @param applicationId the application id
	 * @param fileName the part of a file name
	 * @return the list of log file {@code Resource}s
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static List<Resource> queryContainerLogs(YarnCluster yarnCluster, ApplicationId applicationId, String fileName)
			throws IOException {
		String lastPart = StringUtils.hasText(fileName) ? fileName : "*";
		String pattern = "file:" + yarnCluster.getYarnWorkDir().getAbsolutePath()
				+ "/*logDir*/" + applicationId.toString()
				+ "/**/" + lastPart;
		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		ArrayList<Resource> ret = new ArrayList<Resource>();
		for (Resource r : resolver.getResources(pattern)) {
			if (r.getFile().isFile()) {
				ret.add(r);
			}
		}
		return ret;
	}

	/**
	 * Find container logs for running cluster and application. <code>fileName</code>
	 * will be <code>*</code>.
	 *
	 * @param yarnCluster the yarn cluster
	 * @param applicationId the application id
	 * @return the list of log file {@code Resource}s
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @see #queryContainerLogs(YarnCluster, ApplicationId, String)
	 */
	public static List<Resource> queryContainerLogs(YarnCluster yarnCluster, ApplicationId applicationId)
			throws IOException {
		return queryContainerLogs(yarnCluster, applicationId, null);
	}

	/**
	 * Reads a file content and return it as String. Returns <code>NULL</code>
	 * if file doesn't exist and empty String if file exists but is empty.
	 * @param file the file
	 * @return the file content
	 * @throws Exception the exception if error occurred
	 */
	public static String getFileContent(File file) throws Exception {
		Scanner scanner = null;
		String content = null;
		Exception reThrow = null;
		if (file != null && file.length() > 0) {
			try {
				scanner = new Scanner(file);
				content = scanner.useDelimiter("\\A").next();
			} catch (Exception e) {
				reThrow = e;
			} finally {
				if (scanner != null) {
					try {
						scanner.close();
					} catch (Exception e) {
					}
				}
			}
		} else if (file != null && file.length() == 0) {
			content = "";
		}
		if (reThrow != null) {
			throw reThrow;
		} else {
			return content;
		}
	}

}
