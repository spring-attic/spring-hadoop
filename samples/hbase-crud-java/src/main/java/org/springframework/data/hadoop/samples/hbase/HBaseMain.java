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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 
 * @author Jarred Li
 * 
 */
public class HBaseMain {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) {
		// Initialize spring hadoop application context
		new ClassPathXmlApplicationContext("META-INF/spring/context.xml");

		try {
			//1. create table 
			createTable();

			//2. put data
			putData();

			//3. increment 
			increment();

			//4. get data
			getData();


			//5. scan data
			scanData();

		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}



	}


	/**
	 * 
	 * create HBase table
	 * 
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	private static void createTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {

		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);

		if (admin.tableExists(Constant.tableName)) {
			admin.disableTable(Constant.tableName);
			admin.deleteTable(Constant.tableName);
		}

		HTableDescriptor tableDes = new HTableDescriptor(Constant.tableName);

		HColumnDescriptor cf1 = new HColumnDescriptor(Constant.columnFamilyName);
		tableDes.addFamily(cf1);
		admin.createTable(tableDes);

	}


	/**
	 * put data into table
	 * 
	 * @throws IOException
	 */
	private static void putData() throws IOException {
		Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(config, Constant.tableName);

		for (int i = 0; i < 1000; i++) {
			Put p = new Put(Bytes.toBytes(Constant.rowName + i));
			p.add(Bytes.toBytes(Constant.columnFamilyName), Bytes.toBytes(Constant.linkAddress),
					Bytes.toBytes(Constant.cellValue + i % 7));
			table.put(p);
		}
	}



	/**
	 * get data from table
	 * 
	 * @throws IOException
	 */
	private static void getData() throws IOException {
		Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(config, Constant.tableName);

		Get get = new Get(Bytes.toBytes(Constant.rowName + "2"));
		Result result = table.get(get);
		byte[] valueByte = result.getValue(Bytes.toBytes(Constant.columnFamilyName),
				Bytes.toBytes(Constant.linkAddress));
		System.out.println("get value is:" + new String(valueByte));

		byte[] valueByte2 = result.getValue(Bytes.toBytes(Constant.columnFamilyName),
				Bytes.toBytes(Constant.likeNumber));
		System.out.println("get value is:" + Bytes.toLong(valueByte2));

	}

	/**
	 * increment column value
	 * 
	 * @throws IOException
	 */
	private static void increment() throws IOException {
		Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(config, Constant.tableName);

		for (int i = 0; i < 1000; i++) {
			Increment inc = new Increment(Bytes.toBytes(Constant.rowName + i));
			inc.addColumn(Bytes.toBytes(Constant.columnFamilyName), Bytes.toBytes(Constant.likeNumber), i % 10 + 1);
			table.increment(inc);
		}
	}


	/**
	 * scan data in table
	 * 
	 * @throws IOException
	 */

	private static void scanData() throws IOException {
		Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(config, Constant.tableName);

		Scan scan = new Scan();
		scan.setMaxVersions();
		scan.addColumn(Bytes.toBytes(Constant.columnFamilyName), Bytes.toBytes(Constant.linkAddress));
		//scan.addColumn(Bytes.toBytes(Constant.columnFamilyName), Bytes.toBytes(Constant.likeNumber));
		ResultScanner scanner = table.getScanner(scan);
		for (Result r : scanner) {
			KeyValue[] values = r.raw();
			System.out.println("scanned row:" + Bytes.toString(values[0].getValue()));
			//System.out.println("scanned row:" + Bytes.toLong(values[1].getValue()));
		}
	}



}
