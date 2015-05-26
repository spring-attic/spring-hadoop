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
package org.springframework.data.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for basic HbaseTemplate functionality.
 *
 * @author Thomas Risberg
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HbaseTemplateTest {

	@Resource(name = "hbaseConfiguration")
	Configuration config;

	@Autowired
	HbaseTemplate template;

	String tableName = "testTable";
	String family = "testColumnFamily";
	String rowName = "testRow";
	String qualifier = "testQualifier";
	String qualifier2 = "testQualifier2";
	String value = "Test Value";

	@SuppressWarnings({ "deprecation" })
	@Before
	public void testHBaseConnection() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(config);
		if (admin.tableExists(tableName)) {
			System.out.println("deleting table...");
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}

		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		tableDescriptor.addFamily(new HColumnDescriptor(family));
		assertTrue(tableDescriptor.hasFamily(Bytes.toBytes(family)));
		admin.createTable(tableDescriptor);

		System.out.println("Created table...");
	}

	@Test
	public void testTemplate() throws Exception {

		template.put(tableName, rowName, family, qualifier, Bytes.toBytes(value));
		String results = template.get(tableName, rowName, family, qualifier,
				new RowMapper<String>() {
					@Override
					public String mapRow(Result result, int rowNum) throws Exception {
						return new String(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
					}
				});
		assertEquals(results, value);

		template.delete(tableName, rowName, family, qualifier);
		long count1 = countRowValues(tableName, family);
		assertTrue(count1 == 0);

		template.put(tableName, rowName, family, qualifier, Bytes.toBytes(value));
		template.put(tableName, rowName, family, qualifier2, Bytes.toBytes(value));
		long count2 = countRowValues(tableName, family);
		assertTrue(count2 == 2);
		template.delete(tableName, rowName, family);
		long count3 = countRowValues(tableName, family);
		assertTrue(count3 == 0);

	}

	private long countRowValues(String tableName, final String family) {
		return template.execute(tableName, new TableCallback<Long>() {
			@Override
			public Long doInTable(HTableInterface table) throws Throwable {
				ResultScanner scanner = table.getScanner(new Scan().addFamily(Bytes.toBytes(family)));
				long count = 0;
				while (true) {
					Result r = scanner.next();
					if (r == null) {
						break;
					}
					if (r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)) != null) {
						count++;
					}
					if (r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)) != null) {
						count++;
					}
				}
				return count;
			}
		});
	}
}