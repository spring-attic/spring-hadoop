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
package org.springframework.data.hadoop.fs;

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class FsShellTest {

	{
		TestUtils.hackHadoopStagingOnWin();
	}

	@Before
	public void init() throws Exception {
		TestUtils.writeToFS(cfg, "local/" + UUID.randomUUID() + ".txt");
	}

	@After
	public void destroy() throws Exception {
		FileSystem fs = FileSystem.get(cfg);
		fs.delete(new Path("local/"), true);
	}

	@Autowired
	FsShell shell;
	@Autowired
	Configuration cfg;
	@Autowired
	FileSystem fs;

	@Test
	public void testChmod() throws Exception {
		String name = "local/" + UUID.randomUUID() + ".txt";
		TestUtils.writeToFS(cfg, name);
		FsPermission perm = fs.getFileStatus(new Path(name)).getPermission();
		assertTrue(perm.getGroupAction().implies(FsAction.READ));
		assertTrue(perm.getOtherAction().implies(FsAction.READ));

		shell.chmod("700", name);

		perm = fs.getFileStatus(new Path(name)).getPermission();
		assertTrue(perm.getUserAction().equals(FsAction.READ_WRITE));
		assertTrue(perm.getGroupAction().implies(FsAction.NONE));
		assertTrue(perm.getOtherAction().implies(FsAction.NONE));
	}

	@Test
	public void testCat() throws Exception {
		String name = "local/" + UUID.randomUUID() + ".txt";
		Resource res = TestUtils.writeToFS(cfg, name);
		Collection<Path> cat = shell.cat(name);
		assertEquals(name, cat.toString());
		assertEquals(res.getURI(), cat.iterator().next().toUri());
	}

	@Test
	public void testMultiCat() throws Exception {
		String name1 = "local/" + UUID.randomUUID() + ".txt";
		String name2 = "local/" + UUID.randomUUID() + ".txt";
		Resource res1 = TestUtils.writeToFS(cfg, name1);
		Resource res2 = TestUtils.writeToFS(cfg, name2);
		Collection<Path> cat = shell.cat(name1, name2);
		assertEquals(name1 + "\n" + name2, cat.toString());
		Iterator<Path> it = cat.iterator();
		assertEquals(res1.getURI(), it.next().toUri());
		assertEquals(res2.getURI(), it.next().toUri());
	}

}