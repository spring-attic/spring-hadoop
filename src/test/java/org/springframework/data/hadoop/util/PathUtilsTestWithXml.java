/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * @author Jarred Li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class PathUtilsTestWithXml {

	private static final Log log = LogFactory.getLog(PathUtilsTestWithXml.class);

	@Autowired
	private ApplicationContext ctx;
	private ReferenceClass ref;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link org.springframework.data.haldoop.util.PathUtils#format()}.
	 */
	@Test
	public void testFormat() {
		ref = ctx.getBean("test-ref", ReferenceClass.class);

		String path = ref.getPath();
		Assert.assertNotNull(path);

		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);
		int expectedMonth = cal.get(Calendar.MONTH) + 1;
		int expectedDay = cal.get(Calendar.DAY_OF_MONTH);
		int expectedHour = cal.get(Calendar.HOUR_OF_DAY);

		String appendedStr = path.substring("/home/test".length() + 1);
		log.info("appended str is:" + appendedStr);
		int actualYear = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Year is incorrect", expectedYear, actualYear);

		appendedStr = appendedStr.substring(5);
		log.info("appended str without year is:" + appendedStr);
		int actualMonth = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Month is incorrect", expectedMonth, actualMonth);

		appendedStr = appendedStr.substring(3);
		log.info("appended str without month is:" + appendedStr);
		int actualDay = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Day is incorrect", expectedDay, actualDay);

		appendedStr = appendedStr.substring(3);
		log.info("appended str without day is:" + appendedStr);
		int actualHour = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Hour is incorrect", expectedHour, actualHour);
	}

	@Test
	public void testFormat2() {
		ref = ctx.getBean("test-ref2", ReferenceClass.class);

		String path = ref.getPath();
		Assert.assertNotNull(path);

		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);
		int expectedMonth = cal.get(Calendar.MONTH) + 1;
		int expectedDay = cal.get(Calendar.DAY_OF_MONTH);

		String appendedStr = path.substring("/home/test".length() + 1);
		log.info("appended str is:" + appendedStr);
		int actualYear = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Year is incorrect", expectedYear, actualYear);

		appendedStr = appendedStr.substring(5);
		log.info("appended str without year is:" + appendedStr);
		int actualMonth = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Month is incorrect", expectedMonth, actualMonth);

		appendedStr = appendedStr.substring(3);
		log.info("appended str without month is:" + appendedStr);
		int actualDay = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Day is incorrect", expectedDay, actualDay);

	}

	@Test
	public void testFormat3() {
		ref = ctx.getBean("test-ref3", ReferenceClass.class);

		String path = ref.getPath();
		Assert.assertNotNull(path);

		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);
		int expectedMonth = cal.get(Calendar.MONTH) + 1;

		String appendedStr = path.substring("/home/test".length() + 1);
		log.info("appended str is:" + appendedStr);
		int actualYear = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Year is incorrect", expectedYear, actualYear);

		appendedStr = appendedStr.substring(5);
		log.info("appended str without year is:" + appendedStr);
		int actualMonth = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Month is incorrect", expectedMonth, actualMonth);


	}

	@Test
	public void testFormat4() {
		ref = ctx.getBean("test-ref4", ReferenceClass.class);

		String path = ref.getPath();
		Assert.assertNotNull(path);

		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);

		String appendedStr = path.substring("/home/test".length() + 1);
		log.info("appended str is:" + appendedStr);
		int actualYear = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Year is incorrect", expectedYear, actualYear);


	}


	@Test
	public void testFormat5() {
		ref = ctx.getBean("test-ref5", ReferenceClass.class);

		String path = ref.getPath();
		Assert.assertNotNull(path);

		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);
		int expectedMonth = cal.get(Calendar.MONTH) + 1;
		int expectedDay = cal.get(Calendar.DAY_OF_MONTH);

		String appendedStr = path.substring("/home/test".length() + 1);
		log.info("appended str is:" + appendedStr);

		log.info("appended str without month is:" + appendedStr);
		int actualDay = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Day is incorrect", expectedDay, actualDay);

		appendedStr = appendedStr.substring(3);
		log.info("appended str without year is:" + appendedStr);
		int actualMonth = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Month is incorrect", expectedMonth, actualMonth);

		appendedStr = appendedStr.substring(3);
		int actualYear = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Year is incorrect", expectedYear, actualYear);
	}


	@Test
	public void testFormat6() {
		ref = ctx.getBean("test-ref6", ReferenceClass.class);

		String path = ref.getPath();
		Assert.assertNotNull(path);

		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);
		int expectedMonth = cal.get(Calendar.MONTH) + 1;
		int expectedDay = cal.get(Calendar.DAY_OF_MONTH);

		String appendedStr = path.substring("/home/test".length() + 1);
		log.info("appended str is:" + appendedStr);
		int actualYear = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Year is incorrect", expectedYear, actualYear);

		appendedStr = appendedStr.substring(5);
		log.info("appended str without year is:" + appendedStr);
		int actualMonth = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Month is incorrect", expectedMonth, actualMonth);
		
		appendedStr = appendedStr.substring(3);
		assertTrue(appendedStr.startsWith("data"));
		
		appendedStr = appendedStr.substring(5);
		log.info("appended str without month is:" + appendedStr);
		int actualDay = Integer.parseInt(appendedStr.substring(0, appendedStr.indexOf(File.separator)));
		assertEquals("Day is incorrect", expectedDay, actualDay);

	}


	public static class ReferenceClass {
		private String path;

		/**
		 * @return the path
		 */
		public String getPath() {
			return path;
		}

		/**
		 * @param path the path to set
		 */
		public void setPath(String path) {
			this.path = path;
		}


	}
}
