/*
 * Copyright 2011-2013 the original author or authors.
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

import java.beans.PropertyEditorSupport;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.PropertyEditorRegistrar;
import org.springframework.beans.PropertyEditorRegistry;

/**
 * Registrar handling the MapReduce conversion, typically between {@link Job}s and its related classes.
 * Automatically registered by the Hadoop namespace.
 * 
 * @author Costin Leau
 */
class MapReducePropertyEditorRegistrar implements PropertyEditorRegistrar {

	private abstract static class BaseJobEditor extends PropertyEditorSupport {
		/**
		 * {@inheritDoc}
		 * 
		 * <p/> Converts the given text value to a ServiceReference.
		 */
		public void setAsText(String text) throws IllegalArgumentException {
			throw new IllegalArgumentException("this property editor works only with " + Job.class.getName());
		}

		/**
		 * {@inheritDoc}
		 * 
		 * <p/> This implementation returns <code>null</code> to indicate that
		 * there is no appropriate text representation.
		 */
		public String getAsText() {
			return null;
		}

		public void setValue(Object value) {
			if (value == null) {
				super.setValue(null);
				return;
			}
			if (value instanceof Job) {
				super.setValue(convert((Job) value));
				return;
			}

			throw new IllegalArgumentException("expected a service of type " + Job.class.getName());
		}

		protected abstract Object convert(Job job);
	}


	@Override
	public void registerCustomEditors(PropertyEditorRegistry registry) {

		// Running job is transitive info
		//		registry.registerCustomEditor(RunningJob.class, new BaseJobEditor() {
		//			@Override
		//			protected Object convert(Job job) {
		//				return JobUtils.getRunningJob(job);
		//			}
		//		});

		// Same as this one
		//		registry.registerCustomEditor(JobID.class, new BaseJobEditor() {
		//			@Override
		//			protected Object convert(Job job) {
		//				return JobUtils.getJobId(job);
		//			}
		//		});

		// And this one
		//		registry.registerCustomEditor(org.apache.hadoop.mapred.JobID.class, new BaseJobEditor() {
		//			@Override
		//			protected Object convert(Job job) {
		//				return JobUtils.getOldJobId(job);
		//			}
		//		});


		registry.registerCustomEditor(JobConf.class, new BaseJobEditor() {
			@Override
			protected Object convert(Job job) {
				return JobUtils.getJobConf(job);
			}
		});
	}
}