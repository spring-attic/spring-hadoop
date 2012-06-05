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

package org.springframework.data.hadoop.util;

import java.io.File;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test for PathUtils
 * 
 * @author Jarred Li
 *
 */
public class PathUtilsTest {
	
	private static final Log log = LogFactory.getLog(PathUtilsTest.class);
	
	private PathUtils util;
	
	@Before
	public void before(){
		util = new PathUtils();
	}
	
	@After
	public void after(){
		util = null;
	}
	
	@Test
	public void testGetTimeBasedPathFromRootWithSecond(){
		String rootPath = ".";
		util.setRootPath(rootPath);
		util.setPathFormat("yyyy/MM/dd/HH/mm/ss");
		String result = util.format();
		log.info("path is:" + result);
		assertTrue("time based path is not start with root path",result.startsWith(rootPath));
		
		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);
		int expectedMonth = cal.get(Calendar.MONTH) +1;
		int expectedDay = cal.get(Calendar.DAY_OF_MONTH);
		int expectedHour = cal.get(Calendar.HOUR_OF_DAY);
		
		String appendedStr = result.substring(rootPath.length()+1);
		log.info("appended str is:" + appendedStr);
		int actualYear = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));		
		assertEquals("Year is incorrect",expectedYear,actualYear);
		
		appendedStr = appendedStr.substring(5);
		log.info("appended str without year is:" + appendedStr);
		int actualMonth = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));
		assertEquals("Month is incorrect", expectedMonth, actualMonth);
		
		appendedStr = appendedStr.substring(3);
		log.info("appended str without month is:" + appendedStr);
		int actualDay = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));
		assertEquals("Day is incorrect", expectedDay, actualDay);
		
		appendedStr = appendedStr.substring(3);
		log.info("appended str without day is:" + appendedStr);
		int actualHour = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));
		assertEquals("Hour is incorrect",expectedHour,actualHour);
		
		
	}
	
	@Test
	public void testGetTimeBasedPathFromRootWithMinute(){
		String rootPath = ".";
		util.setRootPath(rootPath);
		util.setPathFormat("yyyy/MM/dd/HH/mm");
		String result = util.format();
		log.info("path is:" + result);
		assertTrue("time based path is not start with root path",result.startsWith(rootPath));
		
		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);
		int expectedMonth = cal.get(Calendar.MONTH) +1;
		int expectedDay = cal.get(Calendar.DAY_OF_MONTH);
		int expectedHour = cal.get(Calendar.HOUR_OF_DAY);
		
		String appendedStr = result.substring(rootPath.length()+1);
		log.info("appended str is:" + appendedStr);
		int actualYear = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));		
		assertEquals("Year is incorrect",expectedYear,actualYear);
		
		appendedStr = appendedStr.substring(5);
		log.info("appended str without year is:" + appendedStr);
		int actualMonth = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));
		assertEquals("Month is incorrect", expectedMonth, actualMonth);
		
		appendedStr = appendedStr.substring(3);
		log.info("appended str without month is:" + appendedStr);
		int actualDay = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));
		assertEquals("Day is incorrect", expectedDay, actualDay);
		
		appendedStr = appendedStr.substring(3);
		log.info("appended str without day is:" + appendedStr);
		int actualHour = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));
		assertEquals("Hour is incorrect",expectedHour,actualHour);
		
		
	}
	
	@Test
	public void testGetTimeBasedPathFromRootWithHour(){
		String rootPath = ".";
		util.setRootPath(rootPath);
		util.setPathFormat("yyyy/MM/dd/HH");
		String result = util.format();
		log.info("path is:" + result);
		assertTrue("time based path is not start with root path",result.startsWith(rootPath));
		
		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);
		int expectedMonth = cal.get(Calendar.MONTH) +1;
		int expectedDay = cal.get(Calendar.DAY_OF_MONTH);
		int expectedHour = cal.get(Calendar.HOUR_OF_DAY);
		
		String appendedStr = result.substring(rootPath.length()+1);
		log.info("appended str is:" + appendedStr);
		int actualYear = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));		
		assertEquals("Year is incorrect",expectedYear,actualYear);
		
		appendedStr = appendedStr.substring(5);
		log.info("appended str without year is:" + appendedStr);
		int actualMonth = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));
		assertEquals("Month is incorrect", expectedMonth, actualMonth);
		
		appendedStr = appendedStr.substring(3);
		log.info("appended str without month is:" + appendedStr);
		int actualDay = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));
		assertEquals("Day is incorrect", expectedDay, actualDay);
		
		appendedStr = appendedStr.substring(3);
		log.info("appended str without day is:" + appendedStr);
		int actualHour = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));
		assertEquals("Hour is incorrect",expectedHour,actualHour);
		
		
	}
	
	@Test
	public void testGetTimeBasedPathFromRootWithDay(){
		String rootPath = ".";
		util.setRootPath(rootPath);
		util.setPathFormat("yyyy/MM/dd");
		String result = util.format();
		log.info("path is:" + result);
		assertTrue("time based path is not start with root path",result.startsWith(rootPath));
		
		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);
		int expectedMonth = cal.get(Calendar.MONTH) +1;
		int expectedDay = cal.get(Calendar.DAY_OF_MONTH);
		
		String appendedStr = result.substring(rootPath.length()+1);
		log.info("appended str is:" + appendedStr);
		int actualYear = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));		
		assertEquals("Year is incorrect",expectedYear,actualYear);
		
		appendedStr = appendedStr.substring(5);
		log.info("appended str without year is:" + appendedStr);
		int actualMonth = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));
		assertEquals("Month is incorrect", expectedMonth, actualMonth);
		
		appendedStr = appendedStr.substring(3);
		log.info("appended str without month is:" + appendedStr);
		int actualDay = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));
		assertEquals("Day is incorrect", expectedDay, actualDay);
		
	
		
	}
	
	
	@Test
	public void testGetTimeBasedPathFromRootWithMonth(){
		String rootPath = ".";
		util.setRootPath(rootPath);
		util.setPathFormat("yyyy/MM");
		String result = util.format();
		log.info("path is:" + result);
		assertTrue("time based path is not start with root path",result.startsWith(rootPath));
		
		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);
		int expectedMonth = cal.get(Calendar.MONTH) +1;

		
		String appendedStr = result.substring(rootPath.length()+1);
		log.info("appended str is:" + appendedStr);
		int actualYear = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));		
		assertEquals("Year is incorrect",expectedYear,actualYear);
		
		appendedStr = appendedStr.substring(5);
		log.info("appended str without year is:" + appendedStr);
		int actualMonth = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));
		assertEquals("Month is incorrect", expectedMonth, actualMonth);
		
		
	}
	
	@Test
	public void testGetTimeBasedPathFromRootWithYear(){
		String rootPath = ".";
		util.setRootPath(rootPath);
		util.setPathFormat("yyyy");
		String result = util.format();
		log.info("path is:" + result);
		assertTrue("time based path is not start with root path",result.startsWith(rootPath));
		
		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);
		
		String appendedStr = result.substring(rootPath.length()+1);
		log.info("appended str is:" + appendedStr);
		int actualYear = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));		
		assertEquals("Year is incorrect",expectedYear,actualYear);
		
		
		
	}
	
	@Test
	public void testGetTimeBasedPathFromRootWithYearAndSecond(){
		String rootPath = ".";
		util.setRootPath(rootPath);
		util.setPathFormat("yyyy/ss");
		String result = util.format();
		log.info("path is:" + result);
		assertTrue("time based path is not start with root path",result.startsWith(rootPath));
		
		Calendar cal = Calendar.getInstance();
		int expectedYear = cal.get(Calendar.YEAR);
		
		String appendedStr = result.substring(rootPath.length()+1);
		log.info("appended str is:" + appendedStr);
		int actualYear = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));		
		assertEquals("Year is incorrect",expectedYear,actualYear);
		
		
		
	}
	
	
	@Test
	public void testGetTimeBasedPathFromRootOnlyHour(){
		String rootPath = ".";
		util.setRootPath(rootPath);
		util.setPathFormat("HH");
		String result = util.format();
		log.info("path is:" + result);
		assertTrue("time based path is not start with root path",result.startsWith(rootPath));
		
		Calendar cal = Calendar.getInstance();
		int expectedHour = cal.get(Calendar.HOUR_OF_DAY);
		
		String appendedStr = result.substring(rootPath.length()+1);
		log.info("appended str is:" + appendedStr);
		int hour = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));		
		assertEquals("hour is incorrect",expectedHour,hour);
		
		
		
	}
	
	@Test
	public void testGetTimeBasedPathFromRootOnlyHourWithUUID(){
		String rootPath = ".";
		util.setRootPath(rootPath);
		util.setPathFormat("HH");
		util.setAppendUUID(true);
		String result = util.format();
		log.info("path is:" + result);
		assertTrue("time based path is not start with root path",result.startsWith(rootPath));
		
		Calendar cal = Calendar.getInstance();
		int expectedHour = cal.get(Calendar.HOUR_OF_DAY);
		
		String appendedStr = result.substring(rootPath.length()+1);
		log.info("appended str is:" + appendedStr);
		int hour = Integer.parseInt(appendedStr.substring(0,appendedStr.indexOf(File.separator)));		
		assertEquals("hour is incorrect",expectedHour,hour);		
		
		
	}
	

}
