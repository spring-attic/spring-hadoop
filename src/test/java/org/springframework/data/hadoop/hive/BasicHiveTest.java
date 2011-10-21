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
package org.springframework.data.hadoop.hive;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * Test for basic JDBC connectivity to Hive following 
 * https://cwiki.apache.org/confluence/display/Hive/HiveClient
 * 
 * @author Costin Leau
 */
public class BasicHiveTest {

	@Test
	public void testHiveConnection() throws Exception {
		GenericXmlApplicationContext ctx = new GenericXmlApplicationContext(
				"/org/springframework/data/hadoop/hive/basic.xml");

		ctx.registerShutdownHook();

		JdbcTemplate jdbc = ctx.getBean("template", JdbcTemplate.class);
		String tableName = "testHiveDriverTable";

		jdbc.execute("drop table " + tableName);
		jdbc.execute("create table " + tableName + " (key int, value string)");
		jdbc.query("show tables", new ResultSetExtractor<String>() {
			public String extractData(ResultSet rs) throws SQLException, DataAccessException {
				System.out.println(rs.getObject(1));
				return "";
			}
		});

		jdbc.query("select count(1) from " + tableName, new ResultSetExtractor<String>() {
			public String extractData(ResultSet rs) throws SQLException, DataAccessException {
				System.out.println(rs.getObject(1));
				return "";
			}
		});

		TTransport transport = new TSocket("localhost", 10000);
		TProtocol protocol = new TBinaryProtocol(transport);
		HiveClient client = new HiveClient(protocol);
		transport.open();

		client.execute("select count(1) as cnt from " + tableName);
		System.out.println(client.fetchOne());
		transport.close();
	}
}