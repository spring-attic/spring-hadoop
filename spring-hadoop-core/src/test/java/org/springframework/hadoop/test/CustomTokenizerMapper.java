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
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	private CustomTokenizer tokenizer = new SimpleTokenizer();

	private Text word = new Text();

	public void setTokenizer(CustomTokenizer tokenizer) {
		this.tokenizer = tokenizer;
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		for (String token : tokenizer.tokenize(value.toString())) {
			word.set(token);
			context.write(word, one);
		}
	}

	public static class SimpleTokenizer implements CustomTokenizer {

		public Iterable<String> tokenize(String input) {
			List<String> list = new ArrayList<String>();
			StringTokenizer outer = new StringTokenizer(input);
			while (outer.hasMoreTokens()) {
				list.add(outer.nextToken());
			}
			return list;
		}

	}
}