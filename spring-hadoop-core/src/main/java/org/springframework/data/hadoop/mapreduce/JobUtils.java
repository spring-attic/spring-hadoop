/*
 * Copyright 2011-2014 the original author or authors.
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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Job.JobState;
import org.apache.hadoop.mapreduce.JobID;
import org.springframework.data.hadoop.configuration.JobConfUtils;
import org.springframework.util.ReflectionUtils;

/**
 * Utilities around Hadoop {@link Job}s.
 * Mainly used for converting a Job instance to different types.
 *
 * @author Costin Leau
 * @author Mark Pollack
 * @author Thomas Risberg
 */
public abstract class JobUtils {

	/**
	 * Status of a job. The enum tries to reuse as much as possible
	 * the internal Hadoop terminology.
	 *
	 * @author Costin Leau
	 */
	public enum JobStatus {
		/**
		 * The status cannot be determined - either because the job might be invalid
		 * or maybe because of a communication failure.
		 */
		UNKNOWN,
		/**
		 * The job is has been/is being defined or configured.
		 * It has not been submitted to the job tracker.
		 */
		DEFINED,
		/**
		 * The job has been submited to the tracker and its execution
		 * is being prepared.
		 */
		PREPARING,
		/**
		 * The job is actually running.
		 */
		RUNNING,
		/**
		 * The execution has completed successfully.
		 */
		SUCCEEDED,
		/**
		 * The execution has failed.
		 */
		FAILED,
		/**
		 * The execution was cancelled or killed.
		 */
		KILLED;

		public static JobStatus fromRunState(int state) {
			switch (state) {
			case 1:
				return RUNNING;
			case 2:
				return SUCCEEDED;
			case 3:
				return FAILED;
			case 4:
				return PREPARING;
			case 5:
				return KILLED;
			default:
				return UNKNOWN;
			}
		}

		public static JobStatus fromJobState(JobState jobState) {
			switch (jobState) {
			case DEFINE:
				return DEFINED;
			case RUNNING:
				return RUNNING;
			default:
				return UNKNOWN;
			}
		}

		public boolean isRunning() {
			return PREPARING == this || RUNNING == this;
		}

		public boolean isFinished() {
			return SUCCEEDED == this || FAILED == this || KILLED == this;
		}

		public boolean isStarted() {
			return DEFINED != this;
		}
	}

	static Field JOB_INFO;
	static Field JOB_CLIENT_STATE;

	static {
		//TODO: remove the need for this
		JOB_CLIENT_STATE = ReflectionUtils.findField(Job.class, "state");
		ReflectionUtils.makeAccessible(JOB_CLIENT_STATE);
	}

	public static RunningJob getRunningJob(Job job) {
		if (job == null) {
			return null;
		}

		try {
			Configuration cfg = job.getConfiguration();
			JobClient jobClient = null;
			try {
				Constructor<JobClient> constr = JobClient.class.getConstructor(Configuration.class);
				jobClient = constr.newInstance(cfg);
			} catch (Exception e) {
				jobClient = new JobClient();
			}
			org.apache.hadoop.mapred.JobID id = getOldJobId(job);
			if (id != null) {
				return jobClient.getJob(id);
			} else {
				return null;
			}
		} catch (IOException e) {
			return null;
		}
	}

	public static JobID getJobId(Job job) {
		if (job == null) {
			return null;
		}
		return job.getJobID();
	}

	public static org.apache.hadoop.mapred.JobID getOldJobId(Job job) {
		if (job == null) {
			return null;
		}
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

		return JobConfUtils.createFrom(configuration, null);
	}

	/**
	 * Returns the status of the given job. May return null indicating accessing the job
	 * caused exceptions.
	 *
	 * @param job the job
	 * @return the job status
	 */
	public static JobStatus getStatus(Job job) {
		if (job == null) {
			return JobStatus.UNKNOWN;
		}

		// attempt to capture the original status
		JobStatus originalStatus = JobStatus.DEFINED;
		try {
			Method getJobState =
					ReflectionUtils.findMethod(Job.class, "getJobState");
			Object state = getJobState.invoke(job);
			if (state instanceof Enum) {
				int value = ((Enum<?>)state).ordinal();
				originalStatus = JobStatus.fromRunState(value + 1);
			}
		} catch (Exception ignore) {}

		// go for the running info if available
		RunningJob runningJob = getRunningJob(job);
		if (runningJob != null) {
			try {
				return JobStatus.fromRunState(runningJob.getJobState());
			} catch (IOException ex) {
				return JobStatus.UNKNOWN;
			}
		}

		// no running info found, assume we can use the original status
		return originalStatus;
	}
}