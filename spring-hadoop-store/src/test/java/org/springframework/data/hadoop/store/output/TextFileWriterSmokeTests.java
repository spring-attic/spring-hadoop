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
package org.springframework.data.hadoop.store.output;

import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.data.hadoop.store.AbstractStoreTests;
import org.springframework.data.hadoop.store.TestUtils;
import org.springframework.data.hadoop.store.event.DefaultStoreEventPublisher;
import org.springframework.data.hadoop.store.event.LoggingListener;
import org.springframework.data.hadoop.store.event.StoreEventPublisher;
import org.springframework.data.hadoop.store.strategy.naming.RollingFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.SizeRolloverStrategy;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.tests.Assume;
import org.springframework.data.hadoop.test.tests.TestGroup;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
@DirtiesContext(classMode=ClassMode.AFTER_EACH_TEST_METHOD)
public class TextFileWriterSmokeTests extends AbstractStoreTests {

	@Autowired
	private ApplicationContext context;

	@Autowired
	private org.apache.hadoop.conf.Configuration hadoopConfiguration;

	private final static String PATH = "/tmp/TextFileWriterSmokeTests/default";

	@Test
	public void testWriteToSingleFile() throws Exception {
		Assume.group(TestGroup.PERFORMANCE);
		int threads = 20;
		int count = 1000;

		TextFileWriter writer = context.getBean("writer1", TextFileWriter.class);
		assertNotNull(writer);

		doConcurrentWrites(writer, threads, count);
		writer.close();

		TestUtils.printLsR(PATH, getConfiguration());
		assertThat(getTotalWritten(), is((long) count * (DATA10.length() + 1) * threads));

		@SuppressWarnings("resource")
		FsShell shell = new FsShell(getConfiguration());
		Collection<FileStatus> files = shell.ls(true, PATH);
		Collection<String> names = statusesToNames(files);
		assertThat(names, everyItem(not(endsWith("tmp"))));

	}

	@Test
	public void testWritesWithRollover() throws Exception {
		Assume.group(TestGroup.PERFORMANCE);

		TextFileWriter writer = context.getBean("writer2", TextFileWriter.class);
		assertNotNull(writer);

		int threads = 5;
		int count = 1000;
		doConcurrentWrites(writer, threads, count);
		writer.close();

		TestUtils.printLsR(PATH, getConfiguration());
		assertThat(getTotalWritten(), is((long) count * (DATA10.length() + 1) * threads));

		@SuppressWarnings("resource")
		FsShell shell = new FsShell(getConfiguration());
		Collection<FileStatus> files = shell.ls(true, PATH);
		Collection<String> names = statusesToNames(files);
		assertThat(names, everyItem(not(endsWith("tmp"))));
	}

	private long getTotalWritten() {
		@SuppressWarnings("resource")
		FsShell shell = new FsShell(hadoopConfiguration);
		long total = 0;
		for (FileStatus s : shell.ls(true, PATH)) {
			if (s.isFile()) {
				total += s.getLen();
			}
		}
		return total;
	}

	private static Collection<String> statusesToNames(Collection<FileStatus> statuses) {
		Collection<String> names = new ArrayList<String>();
		for (FileStatus s : statuses) {
			String p = s.getPath().toString();
			int index = p.indexOf('/', 8);
			names.add(p.substring(index));
		}
		return names;
	}

	private void doConcurrentWrites(final TextFileWriter writer, int threadCount, final int writeCount) {
		final CountDownLatch latch = new CountDownLatch(1);
		final ArrayList<Thread> joins = new ArrayList<Thread>();
		for (int i = 0; i < threadCount; ++i) {
			Runnable runner = new Runnable() {
				public void run() {
					try {
						latch.await();
						for (int j = 0; j < writeCount; j++) {
							TestUtils.writeData(writer, new String[] { DATA10 }, false);
						}
					} catch (Exception ie) {
					}
				}
			};
			Thread t = new Thread(runner, "SmokeThread" + i);
			joins.add(t);
			t.start();
		}
		latch.countDown();
		for (Thread t : joins) {
			try {
				t.join();
			} catch (InterruptedException e) {
			}
		}
	}

	@Configuration
	public static class Config {

		@Autowired
		private org.apache.hadoop.conf.Configuration hadoopConfiguration;

		@Bean
		public TaskExecutor taskExecutor() {
			return new ThreadPoolTaskExecutor();
		}

		@Bean
		public TaskScheduler taskScheduler() {
			return new ThreadPoolTaskScheduler();
		}

		@Bean
		public StoreEventPublisher storeEventPublisher() {
			return new DefaultStoreEventPublisher();
		}

		@Bean
		public LoggingListener loggingListener() {
			return new LoggingListener("INFO");
		}

		@Bean
		public RollingFileNamingStrategy fileNamingStrategy() {
			return new RollingFileNamingStrategy();
		}

		@Bean
		public Path testBasePath() {
			return new Path(PATH);
		}

		@Bean
		public TextFileWriter writer1() {
			TextFileWriter writer = new TextFileWriter(hadoopConfiguration, testBasePath(), null);
			writer.setIdleTimeout(1000);
			writer.setFileNamingStrategy(fileNamingStrategy());
			writer.setInWritingSuffix(".tmp");
			return writer;
		}

		@Bean
		public TextFileWriter writer2() {
			TextFileWriter writer = new TextFileWriter(hadoopConfiguration, testBasePath(), null);
			writer.setIdleTimeout(1000);
			writer.setFileNamingStrategy(fileNamingStrategy());
			writer.setRolloverStrategy(new SizeRolloverStrategy(1000));
			writer.setInWritingSuffix(".tmp");
			return writer;
		}

	}

}
