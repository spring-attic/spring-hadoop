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
package org.springframework.hadoop.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.hadoop.configuration.JobFactoryBean;

/**
 * @author Dave Syer
 * 
 */
@Configuration
public class KitchenSinkConfiguration {
	
	private static Log logger = LogFactory.getLog(KitchenSinkConfiguration.class);

	@Bean
	public FactoryBean<Job> kitchenSinkJob() throws Exception {
		JobFactoryBean factory = new JobFactoryBean();
		factory.setMapper(mapper());
		factory.setReducer(reducer());
		factory.setCombiner(reducer());
		factory.setGroupingComparator(new TextComparator());
		factory.setSortComparator(new TextComparator());
		factory.setInputFormat(inputFormat());
		factory.setOutputFormat(outputFormat());
		factory.setPartitioner(partitioner());
		factory.setOutputKeyClass(Text.class);
		factory.setOutputValueClass(IntWritable.class);
		factory.setInputPaths("foo");
		factory.setOutputPath("target/bar");
		return factory;
	}

	public Partitioner<Text, IntWritable> partitioner() {
		return new SimplePartitioner();
	}

	@Bean
	public InputFormat<Integer, Text> inputFormat() {
		return new SimpleInputFormat();
	}

	@Bean
	public OutputFormat<Text, IntWritable> outputFormat() {
		return new SimpleOutputFormat();
	}

	@Bean
	public Mapper<Object, Text, Text, IntWritable> mapper() {
		return new TokenizerMapper();
	}

	@Bean
	public Reducer<Text, IntWritable, Text, IntWritable> reducer() {
		return new IntSumReducer();
	}

	public static class TextComparator implements RawComparator<Text> {

		@SuppressWarnings("unchecked")
		private RawComparator<Text> delegate = WritableComparator.get(Text.class);

		public int compare(Text o1, Text o2) {
			return delegate.compare(o1, o2);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return delegate.compare(b1, s1, l1, b2, s2, l2);
		}

	}

	public static class SimpleInputFormat extends InputFormat<Integer, Text> {

		public static class FakeInputSplit extends InputSplit implements Writable {

			@Override
			public String[] getLocations() throws IOException, InterruptedException {
				return new String[] { "foo" };
			}

			@Override
			public long getLength() throws IOException, InterruptedException {
				return 0;
			}

			public void write(DataOutput out) throws IOException {
			}

			public void readFields(DataInput in) throws IOException {
			}
		}

		private static class FakeRecordReader extends RecordReader<Integer, Text> {

			private boolean empty = false;

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
					InterruptedException {
			}

			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				boolean success = !empty;
				if (!empty) {
					empty = true;
				}
				return success;
			}

			@Override
			public Integer getCurrentKey() throws IOException, InterruptedException {
				return 1;
			}

			@Override
			public Text getCurrentValue() throws IOException, InterruptedException {
				return new Text("foo");
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return 1;
			}

			@Override
			public void close() throws IOException {
			}

		}

		@Override
		public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
			return Arrays.<InputSplit> asList(new FakeInputSplit());
		}

		@Override
		public RecordReader<Integer, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			return new FakeRecordReader();
		}

	}

	public static class SimpleOutputFormat extends OutputFormat<Text, IntWritable> {

		private final class FakeOutputCommitter extends OutputCommitter {
			@Override
			public void setupTask(TaskAttemptContext taskContext) throws IOException {
			}

			@Override
			public void setupJob(JobContext jobContext) throws IOException {
			}

			@Override
			public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
				return false;
			}

			@Override
			public void commitTask(TaskAttemptContext taskContext) throws IOException {
			}

			@Override
			public void cleanupJob(JobContext jobContext) throws IOException {
			}

			@Override
			public void abortTask(TaskAttemptContext taskContext) throws IOException {
			}
		}

		public static class FakeRecordWriter extends RecordWriter<Text, IntWritable> {

			@Override
			public void write(Text key, IntWritable value) throws IOException, InterruptedException {
				logger.info("Output: " + key + ": " + value);
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			}

		}

		@Override
		public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
				InterruptedException {
			return new FakeRecordWriter();
		}

		@Override
		public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
			return new FakeOutputCommitter();
		}

	}
	
	public static class SimplePartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			return 0;
		}
		
	}

}
