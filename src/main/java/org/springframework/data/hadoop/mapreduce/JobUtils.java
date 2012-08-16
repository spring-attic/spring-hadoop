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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.util.ReflectionUtils;

/**
 * Utilities around Hadoop {@link Job}s.
 * Mainly used for converting a Job instance to different types.
 * 
 * @author Costin Leau
 */
public abstract class JobUtils {

	static Field JOB_INFO;

	static {
		JOB_INFO = ReflectionUtils.findField(Job.class, "info");
		ReflectionUtils.makeAccessible(JOB_INFO);
	}

	public static RunningJob getRunningJob(Job job) {
		if (job == null) {
			return null;
		}

		return (RunningJob) ReflectionUtils.getField(JOB_INFO, job);
	}

	public static JobID getJobId(Job job) {
		if (job == null) {
			return null;
		}

		return job.getJobID();
	}

	public static org.apache.hadoop.mapred.JobID getOldJobId(Job job) {
		JobID id = getJobId(job);
		if (id != null) {
			return org.apache.hadoop.mapred.JobID.downgrade(id);
		}
		return null;
	}

	public static JobConf getJobConf(Job job) {
		if (job == null) {
			return null;
		}

		// we know internally the configuration is a JobConf
		Configuration configuration = job.getConfiguration();

		if (configuration instanceof JobConf) {
			return (JobConf) configuration;
		}

		return (JobConf) ConfigurationUtils.createFrom(configuration, null);
	}
}