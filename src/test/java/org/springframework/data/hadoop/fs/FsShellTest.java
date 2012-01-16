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
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
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
import org.springframework.util.FileSystemUtils;

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

	@Test
	public void testCopyToLocal() throws Exception {
		String fName = UUID.randomUUID() + ".txt";
		String name = "local/" + fName;
		Resource res = TestUtils.writeToFS(cfg, name);
		shell.copyToLocal(name, ".");
		File fl = new File(fName);
		try {
			assertTrue(fl.exists());
			assertArrayEquals(FileCopyUtils.copyToByteArray(res.getInputStream()), FileCopyUtils.copyToByteArray(fl));
		} finally {
			fl.delete();
		}
	}

	@Test
	public void testCopyToLocalMulti() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/" + fName1;
		String fName2 = UUID.randomUUID() + ".txt";
		String name2 = "local/" + fName2;

		File dir = new File("local");
		dir.mkdir();

		Resource res1 = TestUtils.writeToFS(cfg, name1);
		Resource res2 = TestUtils.writeToFS(cfg, name2);
		shell.copyToLocal(name1, "local");
		shell.copyToLocal(false, true, name2, "local");
		File fl1 = new File(name1);
		File fl2 = new File(name2);
		try {
			assertTrue(fl1.exists());
			assertTrue(fl2.exists());
			assertArrayEquals(FileCopyUtils.copyToByteArray(res1.getInputStream()), FileCopyUtils.copyToByteArray(fl1));
			assertArrayEquals(FileCopyUtils.copyToByteArray(res2.getInputStream()), FileCopyUtils.copyToByteArray(fl2));
		} finally {
			FileSystemUtils.deleteRecursively(dir);
		}
	}

	@Test
	public void testCount() throws Exception {
		String name1 = "local/" + UUID.randomUUID() + ".txt";
		String name2 = "local/" + UUID.randomUUID() + ".txt";
		Resource res1 = TestUtils.writeToFS(cfg, name1);
		Resource res2 = TestUtils.writeToFS(cfg, name2);

		Map<Path, ContentSummary> count = shell.count(name1, name2);
		assertTrue(count.size() >= 2);
		for (ContentSummary summary : count.values()) {
			assertEquals(name2.length(), summary.getLength());
		}

		assertTrue(count.toString().contains(name1));
		assertTrue(count.toString().contains(name2));
	}

	@Test
	public void testCountWithQuota() throws Exception {
		String name1 = "local/" + UUID.randomUUID() + ".txt";
		String name2 = "local/" + UUID.randomUUID() + ".txt";
		Resource res1 = TestUtils.writeToFS(cfg, name1);
		Resource res2 = TestUtils.writeToFS(cfg, name2);

		Map<Path, ContentSummary> count = shell.count(true, name1, name2);
		assertTrue(count.size() >= 2);
		for (ContentSummary summary : count.values()) {
			assertEquals(name2.length(), summary.getLength());
		}

		assertTrue(count.toString().contains(name1));
		assertTrue(count.toString().contains(name2));
	}

	@Test
	public void testCp() throws Exception {
		String fName = UUID.randomUUID() + ".txt";
		String name1 = "local/" + fName;
		TestUtils.writeToFS(cfg, name1);

		String dst = "local/cp/";
		shell.mkdir(dst);
		shell.cp(name1, dst);

		assertTrue(shell.test(dst + fName));
		assertEquals(shell.cat(name1).toString(), shell.cat(dst + fName).toString());
	}

	@Test
	public void testCpMulti() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/" + fName1;
		TestUtils.writeToFS(cfg, name1);

		String fName2 = UUID.randomUUID() + ".txt";
		String name2 = "local/" + fName2;
		TestUtils.writeToFS(cfg, name2);

		String dst = "local/cp/";
		shell.mkdir(dst);
		shell.cp(name1, name2, dst);

		assertTrue(shell.test(dst + fName1));
		assertTrue(shell.test(dst + fName2));
		assertEquals(shell.cat(name1).toString(), shell.cat(dst + fName1).toString());
		assertEquals(shell.cat(name2).toString(), shell.cat(dst + fName2).toString());
	}

	@Test
	public void testDUS() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/" + fName1;
		Resource res1 = TestUtils.writeToFS(cfg, name1);

		String fName2 = UUID.randomUUID() + ".txt";
		String name2 = "local/" + fName2;
		Resource res2 = TestUtils.writeToFS(cfg, name2);

		assertEquals(shell.dus(name1).toString(), res1.getURI() + "\t" + name1.length());
		assertEquals(shell.dus(name2).toString(), res2.getURI() + "\t" + name2.length());
	}

	@Test
	public void testDU() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/" + fName1;
		Resource res1 = TestUtils.writeToFS(cfg, name1);

		String fName2 = UUID.randomUUID() + ".txt";
		String name2 = "local/" + fName2;
		Resource res2 = TestUtils.writeToFS(cfg, name2);

		String s = shell.du("local/").toString();
		assertTrue(s.contains(res1.getURI().toString()));
		assertTrue(s.contains(res2.getURI().toString()));
	}

	@Test
	public void testGet() throws Exception {
		testCopyToLocal();
	}

	@Test
	public void testGetMulti() throws Exception {
		testCopyToLocalMulti();
	}

	@Test
	public void testGetMerge() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/merge/" + fName1;
		TestUtils.writeToFS(cfg, name1);

		String fName2 = UUID.randomUUID() + ".txt";
		String name2 = "local/merge/" + fName2;
		TestUtils.writeToFS(cfg, name2);

		File dir = new File("local");
		dir.mkdir();

		String localName = "local/merge.txt";
		File fl1 = new File(localName);

		try {
			shell.getmerge("local/merge/", localName);
			assertTrue(fl1.exists());
			String content = FileCopyUtils.copyToString(new FileReader(fl1));
			assertTrue(content.contains(name1));
			assertTrue(content.contains(name2));
			assertEquals(content.length(), name1.length() + name2.length());
		} finally {
			FileSystemUtils.deleteRecursively(dir);
		}
	}

	@Test
	public void testGetMergeWithNewLine() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/merge/" + fName1;
		TestUtils.writeToFS(cfg, name1);

		String fName2 = UUID.randomUUID() + ".txt";
		String name2 = "local/merge/" + fName2;
		TestUtils.writeToFS(cfg, name2);

		File dir = new File("local");
		dir.mkdir();

		String localName = "local/merge.txt";
		File fl1 = new File(localName);

		try {
			shell.getmerge("local/merge/", localName, true);
			assertTrue(fl1.exists());
			String content = FileCopyUtils.copyToString(new FileReader(fl1));
			assertTrue(content.contains(name1 + "\n"));
			assertTrue(content.contains(name2));
			assertEquals(content.length(), name1.length() + name2.length() + 2);
		} finally {
			FileSystemUtils.deleteRecursively(dir);
		}
	}

	@Test
	public void testLSR() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/merge/" + fName1;
		TestUtils.writeToFS(cfg, name1);

		Collection<FileStatus> lsr = shell.lsr(".");
		assertTrue(lsr.size() > 1);
		String output = lsr.toString();
		assertTrue(output.contains(name1));
	}

	@Test
	public void testLS() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/ls/" + fName1;
		TestUtils.writeToFS(cfg, name1);

		Collection<FileStatus> ls = shell.ls(".");
		assertTrue(ls.size() >= 1);
		assertTrue(ls.toString().contains("drwx"));
		assertFalse(ls.toString().contains(name1));
		ls = shell.ls("local/ls/");
		assertEquals(2, ls.size());
		assertTrue(shell.ls(name1).toString().contains(name1));
	}

	@Test
	public void testMkDir() throws Exception {
		String fname1 = UUID.randomUUID().toString();
		String dir = "local/mkdir-test/" + fname1;
		assertFalse(shell.test(true, false, true, dir));
		shell.mkdir(dir);
		assertTrue(shell.test(true, false, true, dir));
	}
}