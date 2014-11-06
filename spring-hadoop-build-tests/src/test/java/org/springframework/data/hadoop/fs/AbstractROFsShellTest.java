/*
 * Copyright 2011-2014 the original author or authors.
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
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.Assert;

import static org.junit.Assert.*;

/**
 * FsShell ReadOnly integration tests.
 *
 * @author Costin Leau
 */
@ContextConfiguration
public abstract class AbstractROFsShellTest {

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
	protected FsShell shell;
	@Autowired
	protected Configuration cfg;
	@Autowired
	FileSystem hadoopFs;
	BeanFactory bf;

	@Test
	public void testFSImplementation() {
		Assert.isInstanceOf(fsClass(), hadoopFs);
	}

	abstract Class<? extends FileSystem> fsClass();

	@Test
	public void testChmod() throws Exception {
		String name = "local/" + UUID.randomUUID() + ".txt";
		Resource res = TestUtils.writeToFS(cfg, name);
		name = res.getURI().getPath();

		FsPermission perm = hadoopFs.getFileStatus(new Path(name)).getPermission();
		assertTrue(perm.getGroupAction().implies(FsAction.READ));
		assertTrue(perm.getOtherAction().implies(FsAction.READ));

		shell.chmod("600", name);

		perm = hadoopFs.getFileStatus(new Path(name)).getPermission();
		assertTrue(perm.getUserAction().equals(FsAction.READ_WRITE));
		assertTrue(perm.getGroupAction().implies(FsAction.NONE));
		assertTrue(perm.getOtherAction().implies(FsAction.NONE));
	}

	@Test
	public void testCat() throws Exception {
		String originalName = "local/" + UUID.randomUUID() + ".txt";
		Resource res = TestUtils.writeToFS(cfg, originalName);
		String name = res.getURI().getPath();
		Collection<Path> cat = shell.cat(name);
		assertEquals(name, cat.iterator().next().toUri().getPath());
		assertEquals(originalName, cat.toString());
	}

	@Test
	public void testCatMulti() throws Exception {
		String originalName1 = "local/" + UUID.randomUUID() + ".txt";
		String originalName2 = "local/" + UUID.randomUUID() + ".txt";
		Resource res1 = TestUtils.writeToFS(cfg, originalName1);
		Resource res2 = TestUtils.writeToFS(cfg, originalName2);
		String name1 = res1.getURI().getPath();
		String name2 = res2.getURI().getPath();
		Collection<Path> cat = shell.cat(name1, name2);

		assertEquals(originalName1 + "\n" + originalName2, cat.toString());

		Iterator<Path> it = cat.iterator();
		assertEquals(name1, it.next().toUri().getPath());
		assertEquals(name2, it.next().toUri().getPath());
	}

	@Test
	public void testCount() throws Exception {
		String name1 = "local/" + UUID.randomUUID() + ".txt";
		int length1 = name1.length();
		String name2 = "local/" + UUID.randomUUID() + ".txt";
		Resource res1 = TestUtils.writeToFS(cfg, name1);
		name1 = res1.getURI().getPath();
		Resource res2 = TestUtils.writeToFS(cfg, name2);
		name2 = res2.getURI().getPath();

		Map<Path, ContentSummary> count = shell.count(name1, name2);
		assertTrue(count.size() >= 2);
		for (ContentSummary summary : count.values()) {
			assertEquals(length1, summary.getLength());
		}

		assertTrue(count.toString().contains(name1));
		assertTrue(count.toString().contains(name2));
	}

	@Test
	public void testCountWithQuota() throws Exception {
		String name1 = "local/" + UUID.randomUUID() + ".txt";
		String name2 = "local/" + UUID.randomUUID() + ".txt";
		int length1 = name1.length();
		Resource res1 = TestUtils.writeToFS(cfg, name1);
		Resource res2 = TestUtils.writeToFS(cfg, name2);
		name1 = res1.getURI().getPath();
		name2 = res2.getURI().getPath();

		Map<Path, ContentSummary> count = shell.count(true, name1, name2);
		assertTrue(count.size() >= 2);
		for (ContentSummary summary : count.values()) {
			assertEquals(length1, summary.getLength());
		}

		assertTrue(count.toString().contains(name1));
		assertTrue(count.toString().contains(name2));
	}

	@Test
	// TODO: FsShell most likely needs to be rewritten for Hadoop 2
	public void testDUS() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/" + fName1;
		int length1 = name1.length();
		Resource res1 = TestUtils.writeToFS(cfg, name1);
		name1 = res1.getURI().getPath();

		String fName2 = UUID.randomUUID() + ".txt";
		String name2 = "local/" + fName2;
		Resource res2 = TestUtils.writeToFS(cfg, name2);
		name2 = res2.getURI().getPath();

		assertTrue(shell.dus(name1).toString().endsWith(stripPrefix(res1.getURI()) + "\t" + length1));
		assertTrue(shell.dus(name2).toString().endsWith(stripPrefix(res2.getURI()) + "\t" + length1));
	}

	@Test
	public void testDU() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/" + fName1;
		Resource res1 = TestUtils.writeToFS(cfg, name1);
		name1 = res1.getURI().getPath();
		String dir = name1.substring(0, name1.length() - fName1.length());

		String fName2 = UUID.randomUUID() + ".txt";
		String name2 = "local/" + fName2;
		Resource res2 = TestUtils.writeToFS(cfg, name2);
		name2 = res2.getURI().getPath();

		String s = shell.du(dir).toString();
		assertTrue(s.contains(name1));
		assertTrue(s.contains(name2));
	}

	@Test
	public void testLSR() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/lsr/" + fName1;
		Resource res1 = TestUtils.writeToFS(cfg, name1);
		name1 = res1.getURI().getPath();
		String dir = name1.substring(0, name1.length() - fName1.length());

		Collection<FileStatus> lsr = shell.lsr(dir);
		assertTrue(lsr.size() > 1);
		String output = lsr.toString();
		assertTrue(output.contains(name1));
	}

	@Test
	public void testLS() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/ls/" + fName1;
		Resource res1 = TestUtils.writeToFS(cfg, name1);
		name1 = res1.getURI().getPath();

		String dir = name1.substring(0, name1.length() - fName1.length());
		String parentDir = dir.substring(dir.lastIndexOf("/"));

		Collection<FileStatus> ls = shell.ls(parentDir);
		assertTrue(ls.size() >= 1);
		assertTrue(ls.toString().contains("drwx"));
		assertFalse(ls.toString().contains(name1));
		ls = shell.ls(dir);
		assertEquals(2, ls.size());
		assertTrue(shell.ls(name1).toString().contains(name1));
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testSetrep() throws Exception {
		String name1 = "local/setrep/" + UUID.randomUUID() + ".txt";
		Resource res1 = TestUtils.writeToFS(cfg, name1);
		name1 = res1.getURI().getPath();
		Path p = new Path(name1);
		short replication = hadoopFs.getReplication(p);
		shell.setrep((short) (replication + 1), name1);
		assertTrue(replication <= hadoopFs.getReplication(p));
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testMultiSetrep() throws Exception {
		String fName1 = UUID.randomUUID() + ".txt";
		String name1 = "local/setrep/" + fName1;
		String name2 = "local/setrep/" + UUID.randomUUID() + ".txt";

		Resource res1 = TestUtils.writeToFS(cfg, name1);
		Resource res2 = TestUtils.writeToFS(cfg, name2);

		name1 = res1.getURI().getPath();
		name2 = res2.getURI().getPath();
		String dir = name1.substring(0, name1.length() - fName1.length());

		Path p1 = new Path(name1);
		Path p2 = new Path(name2);

		short replication = hadoopFs.getReplication(p1);
		shell.setrep(true, (short) (replication + 1), dir);
		assertTrue(replication <= hadoopFs.getReplication(p1));
		assertTrue(replication <= hadoopFs.getReplication(p2));
	}

	@Test
	public void testTest() throws Exception {
		String name1 = "local/" + UUID.randomUUID() + ".txt";
		assertFalse(shell.test(name1));
		Resource res1 = TestUtils.writeToFS(cfg, name1);
		name1 = res1.getURI().getPath();

		assertTrue(shell.test(name1));
		assertFalse(shell.test(false, true, false, name1));
		assertFalse(shell.test(false, false, true, name1));
	}

	@Test
	public void testTestDir() throws Exception {
		String name1 = "local/" + UUID.randomUUID();
		assertFalse(shell.test(name1));
		Resource res1 = TestUtils.mkdir(cfg, name1);
		name1 = res1.getURI().getPath();
		assertTrue(shell.test(name1));
		assertTrue(shell.test(false, true, false, name1));
		assertTrue(shell.test(false, false, true, name1));
	}

	@Test
	public void testTestFile() throws Exception {
		String name1 = "local/" + UUID.randomUUID();
		assertFalse(shell.test(name1));
		Resource res1 = TestUtils.writeToFS(cfg, name1);
		name1 = res1.getURI().getPath();
		assertTrue(shell.test(name1));
		assertFalse(shell.test(false, false, true, name1));
		assertFalse(shell.test(false, true, false, name1));
	}

	private static String stripPrefix(Object obj) {
		String s = obj.toString();
		s = s.substring(s.lastIndexOf(":"));
		return s.substring(s.indexOf("/"));
	}
}