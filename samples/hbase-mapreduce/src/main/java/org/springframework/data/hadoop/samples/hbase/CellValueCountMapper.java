/*
 * Copyright 2011-2012 the original author or authors.
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

package org.springframework.data.hadoop.samples.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Mapper with HBase to count number of occurence in specified cell
 * 
 * @author Jarred Li
 * 
 */
public class CellValueCountMapper extends TableMapper<Text, IntWritable> {

	private final IntWritable ONE = new IntWritable(1);
	private Text text = new Text();

	public void map(ImmutableBytesWritable row, Result value, Context context)
			throws IOException, InterruptedException {
		if (value != null) {
			byte[] cellValueByte = value.getValue(
					Bytes.toBytes(Constant.columnFamilyName),
					Bytes.toBytes(Constant.qualifierName));
			if (cellValueByte != null) {
				String val = new String(cellValueByte);
				text.set(val);
				context.write(text, ONE);
			}
		}
	}

}
