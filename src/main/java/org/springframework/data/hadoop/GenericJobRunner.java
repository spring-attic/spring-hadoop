/*
 * Copyright 2006-2011 the original author or authors.
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

import org.springframework.util.ClassUtils;

/**
 * A generic main method for launching a Spring-configured Hadoop job. Usage:
 * 
 * <pre>
 * $ hadoop jar &lt;jar-file&gt; org.springframework.hadoop.GenericJobRunner &lt;configLocation&gt; [jobName]
 * </pre>
 * 
 * where
 * 
 * <ul>
 * <li>&lt;jar-file&gt; is the usual Hadoop jar with dependencies (in /lib). The
 * dependencies should include <code>spring-hadoop-core</code> if that is not
 * available in the shared classpath of the Hadoop nodes (which it is not by
 * defautl).</li>
 * <li>&lt;configLocation&gt; is the location of the configuration for the job.
 * It can be a Java package to scan for &#64;Component or &#64;Configuration
 * classes, a &#64;Configuration class name, or an XML file location (ending
 * with <code>.xml</code>).</li>
 * <li>[jobName] is an optional job name. If there is only one job defined in
 * the context created, then that will be used, otherwise you need to specify
 * the job name (as a bean name).
 * </ul>
 * 
 * @author Dave Syer
 * 
 */
public class GenericJobRunner {

	public static void main(String[] args) throws Exception {

		JobTemplate jobTemplate = new JobTemplate();

		boolean success = false;
		String jobName = null;

		if (args.length > 1) {
			jobName = args[1];
		}

		if (args.length == 0) {
			success = jobTemplate.run();
		}
		else {

			try {
				Class<?> configClass = ClassUtils.forName(args[0], ClassUtils.getDefaultClassLoader());
				success = jobTemplate.run(configClass, jobName);
			}
			catch (ClassNotFoundException e) {
				// ignore, and assume it's a package
			}
			success = jobTemplate.run(args[0], jobName);

		}

		if (!success) {
			throw new IllegalStateException("Job failed");
		}
	}
}