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
package org.springframework.data.hadoop.fs;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Collection;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.FileSystemUtils;

import static org.junit.Assert.*;

/**
 * Integration test for FsShell. Note that this test uses FsShell itself inside testing to increase the coverage and reduce the test size.
 *
 * @author Costin Leau
 */
public abstract class AbstractFsShellTest extends AbstractROFsShellTest {

	@Test
	public void testCopyFromLocalAndText() throws Exception {
		String name1 = UUID.randomUUID() + ".txt";
		String dst = "local/" + name1;
		File f = new File(name1);
		f.deleteOnExit();
		FileCopyUtils.copy(name1, new FileWriter(f));

		try {
			shell.copyFromLocal(name1, dst);
			assertTrue(shell.test(dst));
			assertEquals(name1, shell.cat(dst).toString());
			Collection<String> contents =  shell.text(dst);
			assertEquals(name1, contents.iterator().next().toString());
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
	public void testMkDir() throws Exception {
		String fname1 = UUID.randomUUID().toString();
		String dir = "local/mkdir-test/" + fname1;
		assertFalse(shell.test(true, false, true, dir));
		shell.mkdir(dir);
		assertTrue(shell.test(true, false, true, dir));
	}

	@Test
	public void testMoveFromLocal() throws Exception {
		String name1 = UUID.randomUUID() + ".txt";
		String dst = "local/" + name1;
		File f = new File(name1);
		f.deleteOnExit();
		FileCopyUtils.copy(name1, new FileWriter(f));

		try {
			shell.moveFromLocal(name1, dst);
			assertTrue(shell.test(dst));
			assertEquals(name1, shell.cat(dst).toString());
			assertFalse(f.exists());
		} finally {
			f.delete();
		}
	}

	@Test
	public void testMoveFromLocalMultiAndDir() throws Exception {
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
			shell.moveFromLocal(name1, dst);
			shell.moveFromLocal(name2, dst);
			assertTrue(shell.test(dst + name1));
			assertTrue(shell.test(dst + name2));
			assertEquals(name1, shell.cat(dst + name1).toString());
			assertEquals(name2, shell.cat(dst + name2).toString());
			assertFalse(f1.exists());
			assertFalse(f2.exists());
		} finally {
			f1.delete();
			f2.delete();
		}
	}

	@Test
	public void testMvMulti() throws Exception {
		String name1 = UUID.randomUUID() + "-1.txt";
		String name2 = UUID.randomUUID() + "-2.txt";
		String dst = "local/";
		String mv = "local/mv/";

		Resource res1 = TestUtils.writeToFS(cfg, dst + name1);
		Resource res2 = TestUtils.writeToFS(cfg, dst + name2);

		assertTrue(res1.exists());
		assertTrue(res2.exists());

		shell.mkdir(mv);

		shell.mv(dst + name1, mv);
		shell.mv(dst + name2, mv);

		assertTrue(shell.test(mv + name1));
		assertTrue(shell.test(mv + name2));
		assertFalse(shell.test(dst + name1));
		assertFalse(shell.test(dst + name2));
	}

	@Test
	public void testMvSingle() throws Exception {
		String name1 = UUID.randomUUID() + "-1.txt";
		String dst = "local/";
		String mv = "local/mv" + name1;

		Resource res1 = TestUtils.writeToFS(cfg, dst + name1);

		assertTrue(res1.exists());

		shell.mv(dst + name1, mv);

		assertTrue(shell.test(mv));
		assertFalse(shell.test(dst + name1));
	}

	@Test
	public void testPut() throws Exception {
		testCopyFromLocalAndText();
	}

	@Test
	public void testPutMultiAndDir() throws Exception {
		testCopyFromLocalMultiAndDir();
	}

	@Test
	public void testRM() throws Exception {
		String name1 = "local/" + UUID.randomUUID() + ".txt";
		String name2 = "local/" + UUID.randomUUID() + ".txt";

		TestUtils.writeToFS(cfg, name1);
		TestUtils.writeToFS(cfg, name2);

		assertTrue(shell.test(name1));
		assertTrue(shell.test(name2));
		shell.rm(name1, name2);
		assertFalse(shell.test(name1));
		assertFalse(shell.test(name2));
	}

	@Test(expected = IllegalStateException.class)
	public void testRMDirectoryNonRecursive() throws Exception {
		String name1 = "local/rmr/" + UUID.randomUUID() + ".txt";

		TestUtils.writeToFS(cfg, name1);

		assertTrue(shell.test(name1));
		shell.rm("local/rmr/");
	}

	@Test
	public void testRMR() throws Exception {
		String name1 = "local/rmr/" + UUID.randomUUID() + ".txt";
		String name2 = "local/rmr/" + UUID.randomUUID() + ".txt";

		TestUtils.writeToFS(cfg, name1);
		TestUtils.writeToFS(cfg, name2);

		assertTrue(shell.test(name1));
		assertTrue(shell.test(name2));
		shell.rmr("local/rmr/");
		assertFalse(shell.test(name1));
		assertFalse(shell.test(name2));
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testTouchz() throws Exception {
		String name1 = "local/" + UUID.randomUUID() + ".txt";
		assertFalse(shell.test(name1));
		shell.touchz(name1);
		assertTrue(shell.test(true, true, false, name1));
		assertEquals(0, hadoopFs.getLength(new Path(name1)));
	}

	//@Test - disabled for now as Trash is disabled by default as well
	public void testTrash() throws Exception {
		String name1 = "local/rmr/" + UUID.randomUUID() + ".txt";
		TestUtils.writeToFS(cfg, name1);
		shell.rmr("local/rmr/");
		assertTrue(shell.test(".Trash"));
		System.out.println(shell.ls(".Trash"));
	}
}