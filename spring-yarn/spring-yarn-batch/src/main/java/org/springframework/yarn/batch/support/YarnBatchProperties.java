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
package org.springframework.yarn.batch.support;

import java.util.List;
import java.util.Map;

import org.springframework.util.ObjectUtils;

/**
 *
 * @author Janne Valkealahti
 *
 */
public class YarnBatchProperties {

	private String name;
	private boolean enabled = false;
	private List<JobProperties> jobs;
	
	private Map<Jobs2Enum, Object> test;
	
	public void setTest(Map<Jobs2Enum, Object> test) {
		this.test = test;
	}
	
	public Map<Jobs2Enum, Object> getTest() {
		return test;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public List<JobProperties> getJobs() {
		return jobs;
	}

	public void setJobs(List<JobProperties> jobs) {
		this.jobs = jobs;
	}

	/**
	 * Find first occurrence for given job.
	 *
	 * @param jobName the job name
	 * @return the job properties, or <code>null</code> if not found.
	 */
	public JobProperties getJobProperties(String jobName) {
		if (jobs != null) {
			for (JobProperties jp : jobs) {
				if (ObjectUtils.nullSafeEquals(jobName, jp.getName())) {
					return jp;
				}
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return "SpringYarnBatchProperties [jobs=" + jobs + "]";
	}
	
	public static enum Jobs2Enum {
		name,enabled
	}

	/**
	 * Bindings for job properties. This is needed because boot doesn't play
	 * nice with recursive maps.
	 */
	public static class JobProperties {

		/** Job name */
		private String name;

		/** If job is enabled */
		private boolean enabled;

		/** If should try to use JobParametersIncrementer */
		private boolean next;

		/** Should we fail if JobParametersIncrementer cannot be used */
		private boolean failNext;

		/** If we should try to restart a job */
		private boolean restart;

		/** Should we fail if job cannot be restarted */
		private boolean failRestart;

		/** Mapping of job parameters */
		private Map<String, Object> parameters;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public boolean isEnabled() {
			return enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

		public boolean isNext() {
			return next;
		}

		public void setNext(boolean next) {
			this.next = next;
		}

		public boolean isFailNext() {
			return failNext;
		}

		public void setFailNext(boolean failNext) {
			this.failNext = failNext;
		}

		public boolean isRestart() {
			return restart;
		}

		public void setRestart(boolean restart) {
			this.restart = restart;
		}

		public boolean isFailRestart() {
			return failRestart;
		}

		public void setFailRestart(boolean failRestart) {
			this.failRestart = failRestart;
		}

		public Map<String, Object> getParameters() {
			return parameters;
		}

		public void setParameters(Map<String, Object> parameters) {
			this.parameters = parameters;
		}

		@Override
		public String toString() {
			return "Jobs [name=" + name + ", enabled=" + enabled + ", next=" + next + ", failNext=" + failNext
					+ ", restart=" + restart + ", failRestart=" + failRestart + ", parameters=" + parameters + "]";
		}
	}

}
