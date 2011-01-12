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

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * A set of utilities for creating Spring application contexts and pulling out
 * Hadoop components by name or type.
 * 
 * @author Dave Syer
 * 
 */
public interface ContextLoader {

	<T> T getBean(Configuration configuration, Class<T> type, boolean createContext, String property);

	<T> T getBean(Configuration configuration, Class<T> type, boolean createContext);

	void releaseContext(Configuration configuration);
	
	Job getJob(Object configLocation, Properties bootstrap, String jobName);

	void releaseJob(Job job);
	
}
