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

import java.io.File;
import java.io.FileWriter;
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
import org.springframework.util.FileCopyUtils;

import static org.junit.Assert.*;

/**
 * Integration test for FsShell. Note that this test uses FsShell itself inside testing to increase the coverage and reduce the test size.
 * 
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
	public void testCatMulti() throws Exception {
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

	@Test
	public void testCopyFromLocal() throws Exception {
		String name1 = UUID.randomUUID() + ".txt";
		String dst = "local/" + name1;
		File f = new File(name1);
		f.deleteOnExit();
		FileCopyUtils.copy(name1, new FileWriter(f));

		try {
			shell.copyFromLocal(name1, dst);
			assertTrue(shell.test(dst));
			assertEquals(name1, shell.cat(dst).toString());
		} finally {
			f.delete();
		}
	}

	@Test
	public void testCopyFromLocalMultiAndDir() throws Exception {
		String name1 = UUID.randomUUID() + "-1.txt";
		String name2 = UUID.randomUUID() + "-2.txt";
		String dst = "local/";
		File f1 = new File(name1);
		File f2 = new File(name2);
		f1.deleteOnExit();
		f2.deleteOnExit();
		FileCopyUtils.copy(name1, new FileWriter(f1));
		FileCopyUtils.copy(name2, new FileWriter(f2));

		try {
			shell.copyFromLocal(name1, dst);
			shell.copyFromLocal(name2, dst);
			assertTrue(shell.test(dst + name1));
			assertTrue(shell.test(dst + name2));
			assertEquals(name1, shell.cat(dst + name1).toString());
			assertEquals(name2, shell.cat(dst + name2).toString());
		} finally {
			f1.delete();
			f2.delete();
		}
	}
}