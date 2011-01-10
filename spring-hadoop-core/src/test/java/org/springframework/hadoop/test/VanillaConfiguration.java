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

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Dave Syer
 * 
 */
@Configuration
public class VanillaConfiguration extends JobConfiguration {

	@Bean
	@Override
	public Mapper<?, ?, ?, ?> mapper() {
		return new Mapper<Object, Text, Text, IntWritable>() {
			private Text word = new Text();

			private IntWritable one = new IntWritable(1);

			protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				StringTokenizer itr = new StringTokenizer(value.toString());
				while (itr.hasMoreTokens()) {
					word.set(itr.nextToken());
					context.write(word, one);
				}
			}
		};
	}

	@Bean
	@Override
	public Reducer<?, ?, ?, ?> reducer() {
		return new Reducer<Text, IntWritable, Text, IntWritable>() {
			private IntWritable result = new IntWritable();
			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
					InterruptedException {
				int sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
				result.set(sum);
				context.write(key, result);
			}
		};
	}

}
