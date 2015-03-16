/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.hadoop.store.support;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.lang.Override;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;
import org.springframework.data.hadoop.store.TestUtils;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.strategy.naming.ChainedFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.RollingFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.UuidFileNamingStrategy;

/**
 * Tests for {@link OutputStoreObjectSupport}.
 *
 * @author Janne Valkealahti
 *
 */
public class OutputStoreObjectSupportTests {

	@Test
	public void testFindFiles() throws Exception {
		List<FileNamingStrategy> strategies = new ArrayList<FileNamingStrategy>();
		strategies.add(new StaticFileNamingStrategy("base"));
		strategies.add(new UuidFileNamingStrategy("fakeuuid-0", true));
		strategies.add(new RollingFileNamingStrategy());
		strategies.add(new StaticFileNamingStrategy("extension", "."));
		ChainedFileNamingStrategy strategy = new ChainedFileNamingStrategy(strategies);

		TestOutputStoreObjectSupport support = new TestOutputStoreObjectSupport(new Configuration(), new MockPath(1, 1, "/foo"), null);

		support.setInWritingSuffix(".tmp");
		support.setFileNamingStrategy(strategy);

		TestUtils.callMethod("initOutputContext", support);
		assertThat(strategy.resolve(null).toString(), is("base-fakeuuid-0-1.extension"));
	}

	@Test
	public void testFindFiles2() throws Exception {
		List<FileNamingStrategy> strategies = new ArrayList<FileNamingStrategy>();
		strategies.add(new StaticFileNamingStrategy("base"));
		strategies.add(new UuidFileNamingStrategy("fakeuuid-2", true));
		strategies.add(new RollingFileNamingStrategy());
		strategies.add(new StaticFileNamingStrategy("extension", "."));
		ChainedFileNamingStrategy strategy = new ChainedFileNamingStrategy(strategies);

		TestOutputStoreObjectSupport support = new TestOutputStoreObjectSupport(new Configuration(), new MockPath(1, 1, "/foo"), null);

		support.setInWritingSuffix(".tmp");
		support.setFileNamingStrategy(strategy);

		TestUtils.callMethod("initOutputContext", support);
		assertThat(strategy.resolve(null).toString(), is("base-fakeuuid-2-0.extension"));
	}

	@Test
	public void testFindFiles3() throws Exception {
		List<FileNamingStrategy> strategies1 = new ArrayList<FileNamingStrategy>();
		strategies1.add(new StaticFileNamingStrategy("base"));
		strategies1.add(new UuidFileNamingStrategy("fakeuuid-0", true));
		strategies1.add(new RollingFileNamingStrategy());
		strategies1.add(new StaticFileNamingStrategy("extension", "."));
		ChainedFileNamingStrategy strategy1 = new ChainedFileNamingStrategy(strategies1);

		List<FileNamingStrategy> strategies2 = new ArrayList<FileNamingStrategy>();
		strategies2.add(new StaticFileNamingStrategy("base"));
		strategies2.add(new UuidFileNamingStrategy("fakeuuid-1", true));
		strategies2.add(new RollingFileNamingStrategy());
		strategies2.add(new StaticFileNamingStrategy("extension", "."));
		ChainedFileNamingStrategy strategy2 = new ChainedFileNamingStrategy(strategies2);

		MockPath mockPath = new MockPath(2, 2, "/foo");

		TestOutputStoreObjectSupport support1 = new TestOutputStoreObjectSupport(new Configuration(), mockPath, null);
		support1.setInWritingSuffix(".tmp");
		support1.setFileNamingStrategy(strategy1);
		TestUtils.callMethod("initOutputContext", support1);
		assertThat(strategy1.resolve(null).toString(), is("base-fakeuuid-0-2.extension"));

		TestOutputStoreObjectSupport support2 = new TestOutputStoreObjectSupport(new Configuration(), mockPath, null);
		support2.setInWritingSuffix(".tmp");
		support2.setFileNamingStrategy(strategy2);
		TestUtils.callMethod("initOutputContext", support2);
		assertThat(strategy2.resolve(null).toString(), is("base-fakeuuid-1-2.extension"));
	}

	@Test
	public void testRenameWithSuffix() {
		MockPath mockPath = new MockPath(1, 1, "/foo");
		TestOutputStoreObjectSupport support = new TestOutputStoreObjectSupport(new Configuration(), mockPath, null);
		support.setInWritingSuffix(".tmp");
		Path path = new MockPath("/foo/data.txt.tmp");
		assertThat(support.renameFile(path).toString(), is("/foo/data.txt"));
	}

	@Test
	public void testRenameWithPrefix() {
		MockPath mockPath = new MockPath("/foo");
		TestOutputStoreObjectSupport support = new TestOutputStoreObjectSupport(new Configuration(), mockPath, null);
		support.setInWritingPrefix("tmp.");
		Path path = new MockPath("/foo/tmp.data.txt");
		assertThat(support.renameFile(path).toString(), is("/foo/data.txt"));
	}

	private static class TestOutputStoreObjectSupport extends OutputStoreObjectSupport {

		public TestOutputStoreObjectSupport(Configuration configuration, Path basePath, CodecInfo codec) {
			super(configuration, basePath, codec);
		}

	}

	static class MockFileSystem extends RawLocalFileSystem {

		int count = 1;
		int unique = 1;

		public MockFileSystem(int unique, int count) {
			this.unique = unique;
			this.count = count;
		}

		@Override
		public FileStatus[] listStatus(Path pathPattern) throws IOException {
			ArrayList<FileStatus> files = new ArrayList<FileStatus>();
			long modTime = 150;
			for (int j = 0; j<unique; j++) {
				for (int i = 0; i<count; i++) {
					files.add(new FileStatus(10, false, 1, 150, modTime++, new Path("/foo/base-fakeuuid-" + j + "-"+ i + ".extension.tmp")));
				}
			}
			return files.toArray(new FileStatus[0]);
		}

		@Override
		public boolean rename(Path src, Path dst) throws IOException {
			return true;
		}

		@Override
		public boolean exists(Path f) throws IOException {
			return true;
		}

	}

	static class MockPath extends Path {

		int count = 1;
		int unique = 1;

		public MockPath(String pathString) throws IllegalArgumentException {
			super(pathString);
		}

		public MockPath(int unique, int count, String pathString) throws IllegalArgumentException {
			super(pathString);
			this.unique = unique;
			this.count = count;
		}

		@Override
		public FileSystem getFileSystem(Configuration conf) throws IOException {
			return new MockFileSystem(unique, count);
		}

	}
}
