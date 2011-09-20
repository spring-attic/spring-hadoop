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
package org.springframework.data.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * @author Dave Syer
 * 
 */
public class TextInputFormat<K extends Writable, V extends Writable> extends FileInputFormat<K, V> {

	public final LineMapper<K, V> lineMapper;
	
	public TextInputFormat(LineMapper<K, V> lineMapper) {
		this.lineMapper = lineMapper;	
	}

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		LineRecordReader lineRecordReader = new LineRecordReader();
		return new TextInputRecordReader(lineRecordReader);
	}

	private class TextInputRecordReader extends RecordReader<K, V> {

		private LineRecordReader lineRecordReader;
		private V value;
		private K key;

		public TextInputRecordReader(LineRecordReader lineRecordReader) {
			this.lineRecordReader = lineRecordReader;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			lineRecordReader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			boolean result = lineRecordReader.nextKeyValue();
			LongWritable key = lineRecordReader.getCurrentKey();
			Text value = lineRecordReader.getCurrentValue();
			KeyValue<K,V> kv = lineMapper.map(key, value);
			this.key = kv.getKey();
			this.value = kv.getValue();
			return result;
		}

		@Override
		public K getCurrentKey() throws IOException, InterruptedException {
			return this.key;
		}

		@Override
		public V getCurrentValue() throws IOException, InterruptedException {
			return this.value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return lineRecordReader.getProgress();
		}

		@Override
		public void close() throws IOException {
			lineRecordReader.close();
		}

	}

}
