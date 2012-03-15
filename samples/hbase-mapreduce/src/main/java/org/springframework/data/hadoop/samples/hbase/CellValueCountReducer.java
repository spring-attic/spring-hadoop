/*
 * Copyright 2011 the original author or authors.
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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Reducer
 * 
 * @author Jarred Li
 * 
 */
public class CellValueCountReducer extends
		TableReducer<Text, IntWritable, ImmutableBytesWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int i = 0;
		for (IntWritable val : values) {
			i += val.get();
		}
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes(Constant.columnFamilyName),
				Bytes.toBytes("count"), Bytes.toBytes("tatal is:" + i));
		context.write(null, put);
	}

}
