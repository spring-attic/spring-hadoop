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
package org.springframework.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.springframework.hadoop.context.HadoopApplicationContextUtils;

/**
 * @author Dave Syer
 *
 */
public class AutowiringOutputFormat<K, V> extends OutputFormat<K, V> implements Configurable {	
	
	private Configuration configuration;
	private OutputFormat<K,V> delegate;

	public void setConf(Configuration configuration) {
		this.configuration = configuration;
		@SuppressWarnings("unchecked")
		OutputFormat<K,V> bean = HadoopApplicationContextUtils.getBean(configuration, OutputFormat.class);
		delegate = bean;
	}
	
	public Configuration getConf() {
		return configuration;
	}
	
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		if (delegate==null) {
			throw new IllegalStateException("Delegate was not created or configuration not injected");
		}
		return delegate.getRecordWriter(context);
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {		
		if (delegate==null) {
			throw new IllegalStateException("Delegate was not created or configuration not injected");
		}
		delegate.checkOutputSpecs(context);
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		if (delegate==null) {
			throw new IllegalStateException("Delegate was not created or configuration not injected");
		}
		return new ContextAwareOutputCommitter(delegate.getOutputCommitter(context));
	}

	private static class ContextAwareOutputCommitter extends OutputCommitter {

		private final OutputCommitter delegate;

		public ContextAwareOutputCommitter(OutputCommitter delegate) {
			this.delegate = delegate;
		}

		public void setupJob(JobContext jobContext) throws IOException {
			delegate.setupJob(jobContext);
		}

		public void cleanupJob(JobContext jobContext) throws IOException {
			delegate.cleanupJob(jobContext);
		}

		public void setupTask(TaskAttemptContext taskContext) throws IOException {
			delegate.setupTask(taskContext);
		}

		public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
			return delegate.needsTaskCommit(taskContext);
		}

		public void commitTask(TaskAttemptContext taskContext) throws IOException {
			delegate.commitTask(taskContext);
			HadoopApplicationContextUtils.releaseContext(taskContext.getConfiguration());
		}

		@Override
		public void abortTask(TaskAttemptContext taskContext) throws IOException {
			delegate.abortTask(taskContext);
			HadoopApplicationContextUtils.releaseContext(taskContext.getConfiguration());
		}
	}
}
