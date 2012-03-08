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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * 
 * @author Jarred Li
 * 
 */
public class HBaseMR {

	/**
	 * To create Map Reduce Job with input from HBase, output to HBase
	 * 
	 * @param sourceTable
	 * @param targetTable
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void createHBaseMRJob(String sourceTable, String targetTable)
			throws IOException, InterruptedException, ClassNotFoundException {
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);

		if (admin.tableExists(targetTable)) {
			admin.disableTable(targetTable);
			admin.deleteTable(targetTable);
		}

		HTableDescriptor tableDes = new HTableDescriptor(targetTable);
		HColumnDescriptor cf1 = new HColumnDescriptor(Constant.columnFamilyName);
		tableDes.addFamily(cf1);
		admin.createTable(tableDes);

		Job job = new Job(config, "ExampleSummary");
		job.setJarByClass(HBaseMR.class);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(
				sourceTable, // input table
				scan, CellValueCountMapper.class, Text.class,
				IntWritable.class, job);

		TableMapReduceUtil.initTableReducerJob(targetTable,
				CellValueCountReducer.class, job);
		// To put result into HDFS, comment out the following 2 lines of code
		// and comment the above line code.

		// job.setReducerClass(CellValueHDFSReducer.class);
		// FileOutputFormat.setOutputPath(job, new
		// Path("hdfs://localhost:9000/user/hadoop/hbase-output"));

		job.setNumReduceTasks(1);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

	}

}
