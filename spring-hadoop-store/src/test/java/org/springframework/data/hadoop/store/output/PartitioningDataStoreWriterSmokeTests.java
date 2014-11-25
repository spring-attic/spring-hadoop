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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Map;
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
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.Serializer;
import org.springframework.data.hadoop.store.TestUtils;
import org.springframework.data.hadoop.store.event.DefaultStoreEventPublisher;
import org.springframework.data.hadoop.store.event.LoggingListener;
import org.springframework.data.hadoop.store.event.StoreEventPublisher;
import org.springframework.data.hadoop.store.partition.PartitionKeyResolver;
import org.springframework.data.hadoop.store.partition.PartitionResolver;
import org.springframework.data.hadoop.store.partition.PartitionStrategy;
import org.springframework.data.hadoop.store.strategy.naming.RollingFileNamingStrategy;
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
public class PartitioningDataStoreWriterSmokeTests extends AbstractStoreTests {

	@Autowired
	private ApplicationContext context;

	@Autowired
	private org.apache.hadoop.conf.Configuration hadoopConfiguration;

	private final static String PATH = "/tmp/PartitionTextFileWriterSmokeTests/default";

	@Test
	public void testWritePartitions() throws Exception {
		Assume.group(TestGroup.PERFORMANCE);
		int threads = 30;
		int count = 20000;
		int iterations = 10;

		@SuppressWarnings("unchecked")
		PartitioningDataStoreWriter<String, String, String> writer = context.getBean("writer1", PartitioningDataStoreWriter.class);
		assertNotNull(writer);

		for (int i = 0; i < iterations; i++) {
			doConcurrentWrites(writer, threads, count);
			// do sleeps to kick of idle timeout and
			// enough time to rename the file from used postfix
			Thread.sleep(2000);
		}
		Thread.sleep(3000);

		Map<Path, DataStoreWriter<String>> writers = TestUtils.readField("writers", writer);
		assertThat(writers.size(), is(0));

		writer.flush();
		writer.close();

		// assuming items in DATA09ARRAY have same length
		assertThat(getTotalWritten(), is((long) count * (DATA10.length() + 1) * threads * iterations));

	}

	private long getTotalWritten() {
		@SuppressWarnings("resource")
		FsShell shell = new FsShell(hadoopConfiguration);
		long total = 0;
		for (FileStatus s : shell.ls(true, PATH)) {
			System.out.println(s);
			if (s.isFile()) {
				total += s.getLen();
			}
		}
		return total;
	}

	private void doConcurrentWrites(final PartitioningDataStoreWriter<String, String, String> writer, int threadCount, final int writeCount) {
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
		public PartitionStrategy<String, String> partitionStrategy() {
			return new TestPartitionStrategy();
		}

		@Bean
		public PartitioningDataStoreWriter<String, String, String> writer1() {
			
			DataStoreWriterFactory<DataStoreWriter<String>> factory = new TextFileWriterFactory();
			
			ShardedDataStoreWriter<String> shardedWriter = new ShardedDataStoreWriter<String>(hadoopConfiguration,
					testBasePath(), null, factory);
			
			shardedWriter.setIdleTimeout(1000);
			shardedWriter.setFileNamingStrategyFactory(fileNamingStrategy());
			shardedWriter.setInWritingSuffix(".tmp");
			
			Serializer<String, String> serializer = new Serializer<String, String>() {

				@Override
				public String serialize(String entity) {
					return entity;
				}
				
			};
			
			PartitioningDataStoreWriter<String, String, String> writer = new PartitioningDataStoreWriter<String, String, String>(
					shardedWriter,
					partitionStrategy(),
					serializer);
			

			return writer;
		}

	}

}
