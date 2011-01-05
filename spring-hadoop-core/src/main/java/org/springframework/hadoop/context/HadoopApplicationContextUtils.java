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
package org.springframework.hadoop.context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * Static wrapper for a {@link ContextLoader} to enable Spring components to
 * share an application context in a Hadoop environment.
 * 
 * @author Dave Syer
 * 
 */
public class HadoopApplicationContextUtils {

	private static ContextLoader loader = new DefaultContextLoader();

	public static <T> T getBean(Configuration configuration, Class<T> type, String property) {
		return loader.getBean(configuration, type, property);
	}

	public static <T> T getBean(Configuration configuration, Class<T> type) {
		return getBean(configuration, type, null);
	}

	public static void releaseBean(Configuration configuration, Class<?> type, String property) {
		loader.releaseBean(configuration, type, property);
	}

	public static void releaseBean(Configuration configuration, Class<?> type) {
		releaseBean(configuration, type, null);
	}

	public static Job getJob(String configLocation) {
		return loader.getJob(configLocation);
	}

	public static Job getJob(String configLocation, String jobName) {
		return loader.getJob(configLocation, jobName);
	}

	public static void releaseJob(Job job) {
		loader.releaseJob(job);
	}

	public static Job getJob(Class<?> configLocation) {
		return loader.getJob(configLocation);
	}

	public static Job getJob(Class<?> configLocation, String jobName) {
		return loader.getJob(configLocation, jobName);
	}

}
