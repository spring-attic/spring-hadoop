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
import org.apache.thrift.server.TServer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * Test for basic JDBC connectivity to Hive following 
 * https://cwiki.apache.org/confluence/display/Hive/HiveClient
 * 
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/org/springframework/data/hadoop/hive/basic.xml")
public class BasicHiveTest {

	@Autowired
	private ApplicationContext ctx;
	@Autowired
	private HiveOperations hiveTemplate;

	{
		System.setProperty("java.io.tmpdir", "");
		TestUtils.hackHadoopStagingOnWin();
	}

	@Test
	public void testHiveConnection() throws Exception {
		JdbcTemplate jdbc = ctx.getBean("template", JdbcTemplate.class);
		String tableName = "testHiveDriverTable";

		jdbc.execute("drop table if exists " + tableName);
		jdbc.execute("create table " + tableName + " (key int, value string)");
		jdbc.query("show tables", new ResultSetExtractor<String>() {
			public String extractData(ResultSet rs) throws SQLException, DataAccessException {
				return "";
			}
		});

		jdbc.query("select count(1) from " + tableName, new ResultSetExtractor<String>() {
			public String extractData(ResultSet rs) throws SQLException, DataAccessException {
				return "";
			}
		});
	}

	@Test
	public void testQueryForInt() throws Exception {
		String tableName = "testHiveDriverTable";
		hiveTemplate.query("create table if not exists " + tableName + " (key int, value string)");
		System.out.println(hiveTemplate.query("show tables"));
		hiveTemplate.queryForInt("select count(1) as cnt from " + tableName);
	}

	@Test
	public void testQueryForLong() throws Exception {
		String tableName = "testHiveDriverTable";
		//assertEquals(Long.valueOf(1), 
		hiveTemplate.queryForLong("select count(1) as cnt from " + tableName);
		//);
	}

	@Test
	public void testHiveTemplate() throws Exception {
		System.out.println(hiveTemplate.execute(new HiveClientCallback<Object>() {
			@Override
			public Object doInHive(HiveClient hiveClient) throws Exception {
				return hiveClient.get_all_databases();
			}
		}));
	}

	@Test
	// disabled due to antlr incompatibility between Pig (which requires antlr 3.4) and Hive (3.0.x) 
	public void testHiveServer() throws Exception {
		TServer server = ctx.getBean("hive-server", TServer.class);
		assertNotNull(server);
		assertTrue(server.isServing());
	}
}