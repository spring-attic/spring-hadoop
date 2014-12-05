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
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.data.hadoop.store.AbstractStoreTests;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.TestUtils;
import org.springframework.data.hadoop.store.event.DefaultStoreEventPublisher;
import org.springframework.data.hadoop.store.event.LoggingListener;
import org.springframework.data.hadoop.store.event.StoreEventPublisher;
import org.springframework.data.hadoop.store.partition.PartitionKeyResolver;
import org.springframework.data.hadoop.store.partition.PartitionResolver;
import org.springframework.data.hadoop.store.partition.PartitionStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.RollingFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategy;
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

@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class, classes = PartitionTextFileWriterSmokeTests.EmptyConfig.class)
@MiniHadoopCluster
@DirtiesContext(classMode=ClassMode.AFTER_EACH_TEST_METHOD)
public class PartitionTextFileWriterSmokeTests extends AbstractStoreTests {

	@Autowired
	private ApplicationContext context;

	@Autowired
	private org.apache.hadoop.conf.Configuration hadoopConfiguration;

	private final static String PATH1 = "/tmp/PartitionTextFileWriterSmokeTests/testWritePartitions/default";

	private final static String PATH2 = "/tmp/PartitionTextFileWriterSmokeTests/testWritePartitionsWithContextClose/default";

	private final static String PATH3 = "/tmp/PartitionTextFileWriterSmokeTests/testWritePartitionsWithRolloverAndContextClose/default";

	@Test
	public void testWritePartitions() throws Exception {
		Assume.group(TestGroup.PERFORMANCE);

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(BaseConfig.class, Config1.class);
		ctx.refresh();

		int threads = 30;
		int count = 20000;
		int iterations = 10;

		@SuppressWarnings("unchecked")
		PartitionTextFileWriter<String> writer = ctx.getBean("writer1", PartitionTextFileWriter.class);
		assertNotNull(writer);

		for (int i = 0; i < iterations; i++) {
			doConcurrentWrites(writer, threads, count);
			// do sleeps to kick of idle timeout and
			// enough time to rename the file from used postfix
			Thread.sleep(2000);
		}
		Thread.sleep(3000);

		Map<Path, DataStoreWriter<String>> writers = TestUtils.readField("writers", writer);
		TestUtils.printLsR(PATH1, getConfiguration());
		assertThat(writers.size(), is(0));

		writer.flush();
		writer.close();

		// assuming items in DATA09ARRAY have same length
		assertThat(getTotalWritten(PATH1), is((long) count * (DATA10.length() + 1) * threads * iterations));

		@SuppressWarnings("resource")
		FsShell shell = new FsShell(getConfiguration());
		Collection<FileStatus> files = shell.ls(true, PATH1);
		Collection<String> names = statusesToNames(files);
		assertThat(names, everyItem(not(endsWith("tmp"))));

		ctx.close();
	}

	@Test
	public void testWritePartitionsWithContextClose() throws Exception {
		Assume.group(TestGroup.PERFORMANCE);

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(BaseConfig.class, Config2.class);
		ctx.refresh();

		int threads = 30;
		int count = 20000;
		int iterations = 10;

		@SuppressWarnings("unchecked")
		PartitionTextFileWriter<String> writer = ctx.getBean("writer1", PartitionTextFileWriter.class);
		assertNotNull(writer);

		for (int i = 0; i < iterations; i++) {
			doConcurrentWrites(writer, threads, count);
		}

		ctx.close();
		Map<Path, DataStoreWriter<String>> writers = TestUtils.readField("writers", writer);
		TestUtils.printLsR(PATH2, getConfiguration());
		assertThat(writers.size(), is(0));

		// assuming items in DATA09ARRAY have same length
		assertThat(getTotalWritten(PATH2), is((long) count * (DATA10.length() + 1) * threads * iterations));

		@SuppressWarnings("resource")
		FsShell shell = new FsShell(getConfiguration());
		Collection<FileStatus> files = shell.ls(true, PATH2);
		Collection<String> names = statusesToNames(files);
		assertThat(names, everyItem(not(endsWith("tmp"))));

	}

	@Test
	public void testWritePartitionsWithRolloverAndContextClose() throws Exception {
		Assume.group(TestGroup.PERFORMANCE);

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(BaseConfig.class, Config3.class);
		ctx.refresh();

		int threads = 30;
		int count = 20000;
		int iterations = 10;

		@SuppressWarnings("unchecked")
		PartitionTextFileWriter<String> writer = ctx.getBean("writer1", PartitionTextFileWriter.class);
		assertNotNull(writer);

		for (int i = 0; i < iterations; i++) {
			doConcurrentWrites(writer, threads, count);
		}
		Thread.sleep(3000);

		ctx.close();
		Map<Path, DataStoreWriter<String>> writers = TestUtils.readField("writers", writer);
		TestUtils.printLsR(PATH3, getConfiguration());
		assertThat(writers.size(), is(0));

		// assuming items in DATA09ARRAY have same length
		assertThat(getTotalWritten(PATH3), is((long) count * (DATA10.length() + 1) * threads * iterations));

		@SuppressWarnings("resource")
		FsShell shell = new FsShell(getConfiguration());
		Collection<FileStatus> files = shell.ls(true, PATH3);
		Collection<String> names = statusesToNames(files);
		assertThat(names, everyItem(not(endsWith("tmp"))));

	}

	private long getTotalWritten(String path) {
		@SuppressWarnings("resource")
		FsShell shell = new FsShell(hadoopConfiguration);
		long total = 0;
		for (FileStatus s : shell.ls(true, path)) {
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

	private void doConcurrentWrites(final PartitionTextFileWriter<String> writer, int threadCount, final int writeCount) {
		final CountDownLatch latch = new CountDownLatch(1);
		final ArrayList<Thread> joins = new ArrayList<Thread>();
		for (int i = 0; i < threadCount; ++i) {
			Runnable runner = new Runnable() {
				public void run() {
					try {
						latch.await();
						for (int j = 0; j < writeCount; j++) {
							writer.write(DATA09ARRAY[j%DATA09ARRAY.length]);
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

	private static class TestPartitionStrategy implements PartitionStrategy<String, String> {

		TestPartitionResolver partitionResolver = new TestPartitionResolver();
		TestPartitionKeyResolver keyResolver = new TestPartitionKeyResolver();

		@Override
		public PartitionResolver<String> getPartitionResolver() {
			return partitionResolver;
		}

		@Override
		public PartitionKeyResolver<String, String> getPartitionKeyResolver() {
			return keyResolver;
		}
	}

	private static class TestPartitionResolver implements PartitionResolver<String> {

		@Override
		public Path resolvePath(String partitionKey) {
			return new Path(partitionKey);
		}
	}

	private static class TestPartitionKeyResolver implements PartitionKeyResolver<String, String> {

		@Override
		public String resolvePartitionKey(String entity) {
			return entity.substring(0, 2);
		}
	}

	@Configuration
	public static class Config1 {

		@Autowired
		private org.apache.hadoop.conf.Configuration hadoopConfiguration;

		@Bean
		public Path testBasePath() {
			return new Path(PATH1);
		}

		@Bean
		public RollingFileNamingStrategy fileNamingStrategy() {
			return new RollingFileNamingStrategy();
		}

		@Bean
		public PartitionStrategy<String, String> partitionStrategy() {
			return new TestPartitionStrategy();
		}

		@Bean
		public PartitionTextFileWriter<String> writer1() {
			PartitionTextFileWriter<String> writer = new PartitionTextFileWriter<String>(hadoopConfiguration,
					testBasePath(), null, partitionStrategy());
			writer.setIdleTimeout(1000);
			writer.setFileNamingStrategyFactory(fileNamingStrategy());
			writer.setInWritingSuffix(".tmp");
			return writer;
		}

	}

	@Configuration
	public static class Config2 {

		@Autowired
		private org.apache.hadoop.conf.Configuration hadoopConfiguration;

		@Bean
		public Path testBasePath() {
			return new Path(PATH2);
		}

		@Bean
		public RollingFileNamingStrategy fileNamingStrategy() {
			return new RollingFileNamingStrategy();
		}

		@Bean
		public PartitionStrategy<String, String> partitionStrategy() {
			return new TestPartitionStrategy();
		}

		@Bean
		public PartitionTextFileWriter<String> writer1() {
			PartitionTextFileWriter<String> writer = new PartitionTextFileWriter<String>(hadoopConfiguration,
					testBasePath(), null, partitionStrategy());
			writer.setIdleTimeout(60000);
			writer.setFileNamingStrategyFactory(fileNamingStrategy());
			writer.setInWritingSuffix(".tmp");
			return writer;
		}

	}

	@Configuration
	public static class Config3 {

		@Autowired
		private org.apache.hadoop.conf.Configuration hadoopConfiguration;

		@Bean
		public Path testBasePath() {
			return new Path(PATH3);
		}

		@Bean
		public FileNamingStrategy fileNamingStrategy() {
			return new RollingFileNamingStrategy();
		}

		@Bean
		public RolloverStrategy rolloverStrategy() {
			return new SizeRolloverStrategy("1M");
		}

		@Bean
		public PartitionStrategy<String, String> partitionStrategy() {
			return new TestPartitionStrategy();
		}

		@Bean
		public PartitionTextFileWriter<String> writer1() {
			PartitionTextFileWriter<String> writer = new PartitionTextFileWriter<String>(hadoopConfiguration,
					testBasePath(), null, partitionStrategy());
			writer.setIdleTimeout(1000);
			writer.setFileNamingStrategyFactory(fileNamingStrategy());
			writer.setRolloverStrategyFactory(rolloverStrategy());
			writer.setInWritingSuffix(".tmp");
			return writer;
		}

	}

	@Configuration
	public static class BaseConfig {

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

	}

	@Configuration
	static class EmptyConfig {
	}

}
