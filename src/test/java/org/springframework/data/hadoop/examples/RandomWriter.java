/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.mapreduce.JobTemplate;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

public class RandomWriter extends Configured implements Tool, InitializingBean {

	private static final String SPRING_CONFIG_LOCATION = "classpath:/"
			+ ClassUtils.addResourcePathToPackagePath(RandomWriter.class, "RandomWriter-context.xml");

	public static final String NUM_MAPS = "mapreduce.job.maps";

	private long numBytesToWrite;

	private long totalBytesToWrite;

	private int minKeySize;

	private int maxKeySize;

	private int minValueSize;

	private int maxValueSize;

	private int mapsPerHost;

	static enum Counters {
		RECORDS_WRITTEN, BYTES_WRITTEN
	}

	public void setNumBytesToWrite(long numBytesToWrite) {
		this.numBytesToWrite = numBytesToWrite;
	}

	public void setTotalBytesToWrite(long totalBytesToWrite) {
		this.totalBytesToWrite = totalBytesToWrite;
	}

	public void setMinKeySize(int minKeySize) {
		this.minKeySize = minKeySize;
	}

	public void setMaxKeySize(int maxKeySize) {
		this.maxKeySize = maxKeySize;
	}

	public void setMinValueSize(int minValueSize) {
		this.minValueSize = minValueSize;
	}

	public void setMaxValueSize(int maxValueSize) {
		this.maxValueSize = maxValueSize;
	}

	public void setMapsPerHost(int mapsPerHost) {
		this.mapsPerHost = mapsPerHost;
	}

	public void afterPropertiesSet() throws Exception {

		Assert.state(numBytesToWrite > 0, "The value of numBytesToWrite must be greater than 0");
		Assert.state(minKeySize >= 0, "The value of minKeySize must be greater than or equal to 0");
		Assert.state(maxKeySize > minKeySize, "The maxKeySize must be greater than minKeySize");
		Assert.state(minValueSize >= 0, "The value of minValueSize must be greater than or equal to 0");
		Assert.state(mapsPerHost > 0, "The value of mapsPerHost must be greater than 0");

		Configuration configuration = getConf();
		@SuppressWarnings("deprecation")
		JobClient client = new JobClient(new org.apache.hadoop.mapred.JobConf(configuration));
		ClusterStatus cluster = client.getClusterStatus();
		long actualTotalBytesToWrite = totalBytesToWrite >= numBytesToWrite ? totalBytesToWrite : mapsPerHost
				* numBytesToWrite * cluster.getTaskTrackers();
		configuration.setInt(NUM_MAPS, (int) (actualTotalBytesToWrite / numBytesToWrite));

	}

	private static class RandomInputFormat extends InputFormat<Text, Text> {

		/**
		 * Generate the requested number of file splits, with the filename set
		 * to the filename of the output file.
		 */
		public List<InputSplit> getSplits(JobContext job) throws IOException {
			List<InputSplit> result = new ArrayList<InputSplit>();
			Path outDir = FileOutputFormat.getOutputPath(job);
			int numSplits = job.getConfiguration().getInt(NUM_MAPS, 1);
			for (int i = 0; i < numSplits; ++i) {
				result.add(new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, (String[]) null));
			}
			return result;
		}

		private static class RandomRecordReader extends RecordReader<Text, Text> {
			Path name;

			Text key = null;

			Text value = new Text();

			public RandomRecordReader(Path p) {
				name = p;
			}

			public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
					InterruptedException {

			}

			public boolean nextKeyValue() {
				if (name != null) {
					key = new Text();
					key.set(name.getName());
					name = null;
					return true;
				}
				return false;
			}

			public Text getCurrentKey() {
				return key;
			}

			public Text getCurrentValue() {
				return value;
			}

			public void close() {
			}

			public float getProgress() {
				return 0.0f;
			}
		}

		public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			return new RandomRecordReader(((FileSplit) split).getPath());
		}
	}

	private class RandomMapper extends Mapper<WritableComparable<?>, Writable, BytesWritable, BytesWritable> {

		private Random random = new Random();

		private BytesWritable randomKey = new BytesWritable();

		private BytesWritable randomValue = new BytesWritable();

		private int keySizeRange = maxKeySize - minKeySize;

		private int valueSizeRange = maxValueSize - minValueSize;

		private void randomizeBytes(byte[] data, int offset, int length) {
			for (int i = offset + length - 1; i >= offset; --i) {
				data[i] = (byte) random.nextInt(256);
			}
		}

		/**
		 * Given an output filename, write a bunch of random records to it.
		 */
		public void map(WritableComparable<?> key, Writable value, Context context) throws IOException,
				InterruptedException {
			int itemCount = 0;
			while (numBytesToWrite > 0) {
				int keyLength = minKeySize + (keySizeRange != 0 ? random.nextInt(keySizeRange) : 0);
				randomKey.setSize(keyLength);
				randomizeBytes(randomKey.getBytes(), 0, randomKey.getLength());
				int valueLength = minValueSize + (valueSizeRange != 0 ? random.nextInt(valueSizeRange) : 0);
				randomValue.setSize(valueLength);
				randomizeBytes(randomValue.getBytes(), 0, randomValue.getLength());
				context.write(randomKey, randomValue);
				numBytesToWrite -= keyLength + valueLength;
				context.getCounter(Counters.BYTES_WRITTEN).increment(keyLength + valueLength);
				context.getCounter(Counters.RECORDS_WRITTEN).increment(1);
				if (++itemCount % 200 == 0) {
					context.setStatus("wrote record " + itemCount + ". " + numBytesToWrite + " bytes left.");
				}
			}
			context.setStatus("done with " + itemCount + " records.");
		}
	}

	public Class<?> getMapper() {
		return RandomMapper.class;
	}

	public Class<?> getInputFormat() {
		return RandomInputFormat.class;
	}

	@Override
	public Configuration getConf() {
		Configuration configuration = super.getConf();
		if (configuration == null) {
			configuration = new Configuration();
		}
		return configuration;
	}

	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();
		JobTemplate jobTemplate = new JobTemplate();
		jobTemplate.setConfiguration(configuration);
		return jobTemplate.run(SPRING_CONFIG_LOCATION) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RandomWriter(), args);
		System.exit(res);
	}

}
