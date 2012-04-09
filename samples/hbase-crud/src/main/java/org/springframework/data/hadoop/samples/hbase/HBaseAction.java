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

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Component;

/**
 * Demo class counting occurrances in HBase through SHDP HBaseTemplate. 
 * 
 * @author Costin Leau
 * @author Jarred Li
 */
@Component
public class HBaseAction {

	@Autowired
	private Configuration hbaseConfiguration;

	@Inject
	private HbaseTemplate t;

	String rowName = "row";
	String likeNumber = "likeCount";
	String linkAddress = "link";
	String columnFamilyName = "cf";
	String tableName = "likes";
	String cellValue = "http://blog.springsource.org/2012/02/29/introducing-spring-hadoop/";

	/**
	 * Main entry point.
	 */
	@PostConstruct
	public void run() throws Exception {
		//1. create table 
		createTable();

		//2. put data
		addData();

		//3. increment 
		increment();

		//4. get 1 column data
		readData();

		//5. scan data
		scanData();
	}

	/**
	 * Creates HBase table
	 *
	 * @throws Exception
	 */
	private void createTable() throws Exception {

		HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);

		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}

		HTableDescriptor tableDes = new HTableDescriptor(tableName);

		HColumnDescriptor cf1 = new HColumnDescriptor(columnFamilyName);
		tableDes.addFamily(cf1);
		admin.createTable(tableDes);
	}


	/**
	 * Adds some sample data.
	 */
	private void addData() {
		t.execute(tableName, new TableCallback<Object>() {
			public Object doInTable(HTable table) throws Throwable {
				for (int i = 0; i < 1000; i++) {
					Put p = new Put(Bytes.toBytes(rowName + i));
					p.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(linkAddress), Bytes.toBytes(cellValue + i % 7));
					table.put(p);
				}
				return null;
			}
		});
	}



	/**
	 * Read data from table.
	 * 
	 */
	private void readData() {
		// replace with get 
		System.out.println(t.execute(tableName, new TableCallback<Long>() {
			public Long doInTable(HTable table) throws Throwable {
				Get get = new Get(Bytes.toBytes(rowName + "2"));
				Result result = table.get(get);
				byte[] valueByte = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(linkAddress));
				return Bytes.toLong(valueByte);
			}
		}));
	}

	/**
	 * Update column values.
	 */
	private void increment() {
		t.execute(tableName, new TableCallback<Object>() {
			public Object doInTable(HTable table) throws Throwable {
				for (int i = 0; i < 1000; i++) {
					Increment inc = new Increment(Bytes.toBytes(rowName + i));
					inc.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(likeNumber), i % 10 + 1);
					table.increment(inc);
				}
				return null;
			}
		});
	}


	/**
	 * Scan table data.
	 * 
	 */

	private void scanData() {

		t.query(tableName, columnFamilyName, linkAddress, new RowMapper<String>() {
			public String mapRow(Result result, int rowNum) throws Exception {
				return Bytes.toString(result.value());
			}
		});
	}
}