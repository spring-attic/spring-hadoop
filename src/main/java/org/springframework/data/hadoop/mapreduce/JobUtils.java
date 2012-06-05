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
package org.springframework.data.hadoop.mapreduce;

import java.lang.reflect.Field;

import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.util.ReflectionUtils;

/**
 * Utils around Hadoop {@link Job}s.
 * 
 * @author Costin Leau
 */
abstract class JobUtils {

	static RunningJob getRunningJob(Job job) {
		Field f = ReflectionUtils.findField(Job.class, "info");
		ReflectionUtils.makeAccessible(f);

		return (RunningJob) ReflectionUtils.getField(f, job);
	}
}
