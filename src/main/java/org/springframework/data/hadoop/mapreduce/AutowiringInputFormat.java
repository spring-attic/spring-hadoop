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
package org.springframework.data.hadoop.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.springframework.data.hadoop.context.HadoopApplicationContextUtils;

/**
 * @author Dave Syer
 * 
 */
public class AutowiringInputFormat<K, V> extends InputFormat<K, V> implements Configurable {

	private Configuration configuration;

	private InputFormat<K, V> delegate;

	public void setConf(Configuration configuration) {
		this.configuration = configuration;
		@SuppressWarnings("unchecked")
		InputFormat<K, V> bean = HadoopApplicationContextUtils.getBean(configuration, InputFormat.class);
		delegate = bean;
	}

	public Configuration getConf() {
		return configuration;
	}

	@Override
	public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
		return delegate.getSplits(context);
	}

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new ContextAwareRecordReader<K, V>(delegate.createRecordReader(split, context), context.getConfiguration());
	}

	private static class ContextAwareRecordReader<K, V> extends RecordReader<K, V> {

		private final RecordReader<K, V> delegate;
		private final Configuration configuration;

		public ContextAwareRecordReader(RecordReader<K, V> delegate, Configuration configuration) {
			this.delegate = delegate;
			this.configuration = configuration;
		}

		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			delegate.initialize(split, context);
		}

		public boolean nextKeyValue() throws IOException, InterruptedException {
			return delegate.nextKeyValue();
		}

		public K getCurrentKey() throws IOException, InterruptedException {
			return delegate.getCurrentKey();
		}

		public V getCurrentValue() throws IOException, InterruptedException {
			return delegate.getCurrentValue();
		}

		public float getProgress() throws IOException, InterruptedException {
			return delegate.getProgress();
		}

		public void close() throws IOException {
			delegate.close();
			HadoopApplicationContextUtils.releaseContext(configuration);
		}

	}

}
