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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.springframework.util.StringUtils;

/**
 * @author Dave Syer
 * 
 */
public class SeparatorLineMapper implements LineMapper<Text, Text> {

	private static final String DEFAULT_DELIMITER = "\t";

	private String delimiter = DEFAULT_DELIMITER;

	/**
	 * @param delimiter the delimiter to set
	 */
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public KeyValue<Text, Text> map(LongWritable key, Text value) {
		if (value==null || value.getLength()==0) {
			return new KeyValue<Text, Text>(new Text(""), new Text(""));
		}
		String input = value.toString();
		String[] split = StringUtils.split(input, delimiter);
		Text outValue = new Text(split.length > 1 ? split[1] : "");
		Text outKey = new Text(split[0]);
		return new KeyValue<Text, Text>(outKey, outValue);
	}

}
