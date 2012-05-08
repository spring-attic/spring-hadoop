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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;

/**
 * 
 * @author Jarred Li
 * 
 */
public class HBaseMain {

	/**
	 * @param args
	 * @throws Exception 
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {
		// Initialize spring hadoop application context
		ApplicationContext ctx = new ClassPathXmlApplicationContext("META-INF/spring/context.xml");

		Configuration config = HBaseConfiguration.create();

		createTableAndInitData(Constant.tableName, Constant.columnFamilyName, Constant.qualifierName);
		initTargetTable(config, Constant.targetTableName);
		JobRunner runner = ctx.getBean("runner", JobRunner.class);
		runner.runJobs();

		checkValue(Constant.targetTableName, config);

	}

	/**
	 * init source table
	 * 
	 * @param tableName
	 * @param cfName
	 * @param qualifier
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	private static void createTableAndInitData(String tableName, String cfName, String qualifier)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {

		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);

		String rowName = "row";
		String value = "http://blog.springsource.org/2012/02/29/introducing-spring-hadoop/";

		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}

		HTableDescriptor tableDes = new HTableDescriptor(tableName);
		HColumnDescriptor cf1 = new HColumnDescriptor(cfName);
		tableDes.addFamily(cf1);
		admin.createTable(tableDes);

		HTable table = new HTable(config, tableName);

		for (int i = 0; i < 1000; i++) {
			Put p = new Put(Bytes.toBytes(rowName + i));
			p.add(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), Bytes.toBytes(value + i % 7));
			table.put(p);
		}
	}

	/**
	 * init target table to store result
	 * 
	 * @param config
	 * @param targetTable
	 * @throws IOException
	 */
	private static void initTargetTable(Configuration config, String targetTable) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(config);

		if (admin.tableExists(targetTable)) {
			admin.disableTable(targetTable);
			admin.deleteTable(targetTable);
		}

		HTableDescriptor tableDes = new HTableDescriptor(targetTable);
		HColumnDescriptor cf1 = new HColumnDescriptor(Constant.columnFamilyName);
		tableDes.addFamily(cf1);
		admin.createTable(tableDes);

	}

	/**
	 * check the value in the target table.
	 * 
	 * @param targetTable
	 * @param config
	 * @throws IOException
	 */
	private static void checkValue(String targetTable, Configuration config) throws IOException {
		HTable table = new HTable(config, targetTable);

		Scan scanResult = new Scan();
		scanResult.addColumn(Bytes.toBytes(Constant.columnFamilyName), Bytes.toBytes("count"));
		ResultScanner scanner = table.getScanner(scanResult);
		for (Result r : scanner) {
			System.out.println(new String(r.getRow()) + ": " + new String(r.value()));
		}
	}

}
