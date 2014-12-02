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
package org.springframework.data.hadoop.store;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.data.hadoop.store.config.annotation.EnableDataStoreTextWriter;
import org.springframework.data.hadoop.store.config.annotation.SpringDataStoreTextWriterConfigurerAdapter;
import org.springframework.data.hadoop.store.config.annotation.builders.DataStoreTextWriterConfigurer;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(loader = HadoopDelegatingSmartContextLoader.class, classes = ContextCloseWriterTests.EmptyConfig.class)
@MiniHadoopCluster
public class ContextCloseWriterTests extends AbstractStoreTests {

	@Autowired
	private ApplicationContext context;

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	@Test
	public void testSingleTextFileWriterCloseManually() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config1.class);
		ctx.refresh();
		DataStoreWriter writer1 = ctx.getBean("writer1", DataStoreWriter.class);
		writer1.write("foo");
		writer1.close();

		FsShell shell = new FsShell(getConfiguration());

		Collection<FileStatus> files = shell.ls(true, Config1.PATH);
		assertThat(files.size(), is(1));
		assertThat(files.iterator().next().getLen(), is(4l));
		ctx.close();
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	@Test
	public void testSingleTextFileWriterCloseByContext() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config2.class);
		ctx.refresh();
		DataStoreWriter writer2 = ctx.getBean("writer2", DataStoreWriter.class);
		writer2.write("foo");
		ctx.close();

		FsShell shell = new FsShell(getConfiguration());
		Collection<FileStatus> files = shell.ls(true, Config2.PATH);
		assertThat(files.size(), is(1));
		assertThat(files.iterator().next().getLen(), is(4l));
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	@Test
	public void testSingleTextFileWriterInUsePostfixCloseByContext() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config3.class);
		ctx.refresh();
		DataStoreWriter writer3 = ctx.getBean("writer3", DataStoreWriter.class);
		writer3.write("foo");

		FsShell shell = new FsShell(getConfiguration());
		Collection<FileStatus> files = shell.ls(true, Config3.PATHINUSE);
		assertThat(files.size(), is(1));
		// 0 because data not flushed
		assertThat(files.iterator().next().getLen(), is(0l));

		ctx.close();

		files = shell.ls(true, Config3.PATH);
		assertThat(files.size(), is(1));
		assertThat(files.iterator().next().getLen(), is(4l));
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	@Test
	public void testSinglePartitionTextFileWriterInUsePostfixCloseByContext() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config4.class);
		ctx.refresh();
		PartitionDataStoreWriter writer4 = ctx.getBean("writer4", PartitionDataStoreWriter.class);

		Map<String, Object> partitionKey = new HashMap<String, Object>();
		partitionKey.put("timestamp", 0);
		writer4.write("foo", partitionKey);

		partitionKey.put("timestamp", 1000);
		writer4.write("foo", partitionKey);

		FsShell shell = new FsShell(getConfiguration());
		Collection<FileStatus> files = shell.ls(true, Config4.DIR);
		Collection<String> names = statusesToNames(files);
		assertThat(files.size(), is(5));
		assertThat(
				names,
				containsInAnyOrder(Config4.DIR, Config4.DIR + "/0", Config4.DIR + "/0/data.tmp", Config4.DIR + "/1000",
						Config4.DIR + "/1000/data.tmp"));

		ctx.close();
		files = shell.ls(true, Config4.DIR);
		names = statusesToNames(files);
		assertThat(files.size(), is(5));
		assertThat(
				names,
				containsInAnyOrder(Config4.DIR, Config4.DIR + "/0", Config4.DIR + "/0/data", Config4.DIR + "/1000",
						Config4.DIR + "/1000/data"));
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	@Test
	public void testSinglePartitionTextFileWriterComplexNamingCloseByContext() throws Exception {
		// this test is a bit tricky because original problem of a file rename for
		// in-use-postfix happend in a separate thread and context/jvm close
		// may kill that thread. I was able to reproduce this trouble with
		// 2sec sleep here with 1 sec idleTimeout. Thought tests didn't fail
		// every time. So this is better than nothing for catching this problem.
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config5.class);
		ctx.refresh();
		PartitionDataStoreWriter writer5 = ctx.getBean("writer5", PartitionDataStoreWriter.class);

		Map<String, Object> partitionKey = new HashMap<String, Object>();
		partitionKey.put("timestamp", 0);
		writer5.write("foo", partitionKey);

		partitionKey.put("timestamp", 1000);
		writer5.write("foo", partitionKey);

		FsShell shell = new FsShell(getConfiguration());
		Collection<FileStatus> files = shell.ls(true, Config5.DIR);
		Collection<String> names = statusesToNames(files);
		assertThat(files.size(), is(5));

		// context close should rename the files
		ctx.close();

		files = shell.ls(true, Config5.DIR);
		names = statusesToNames(files);
		assertThat(files.size(), is(5));
		assertThat(
				names,
				containsInAnyOrder(Config5.DIR, Config5.DIR + "/0", Config5.DIR + "/0/data-0.txt", Config5.DIR + "/1000",
						Config5.DIR + "/1000/data-0.txt"));
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	@Test
	public void testSinglePartitionTextFileWriterComplexNamingCloseByTimeout() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config6.class);
		ctx.refresh();
		PartitionDataStoreWriter writer6 = ctx.getBean("writer6", PartitionDataStoreWriter.class);

		Map<String, Object> partitionKey = new HashMap<String, Object>();
		partitionKey.put("timestamp", 0);
		writer6.write("foo", partitionKey);

		partitionKey.put("timestamp", 1000);
		writer6.write("foo", partitionKey);

		Thread.sleep(3000);

		FsShell shell = new FsShell(getConfiguration());
		Collection<FileStatus> files = shell.ls(true, Config6.DIR);
		Collection<String> names = statusesToNames(files);
		assertThat(files.size(), is(5));

		files = shell.ls(true, Config6.DIR);
		names = statusesToNames(files);
		assertThat(files.size(), is(5));
		assertThat(
				names,
				containsInAnyOrder(Config6.DIR, Config6.DIR + "/0", Config6.DIR + "/0/data-0.txt", Config6.DIR + "/1000",
						Config6.DIR + "/1000/data-0.txt"));
		// rename should have happened before context close
		ctx.close();
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	@Test
	public void testPartitionTextFileWriterPrefixRemoved() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config7.class);
		ctx.refresh();
		PartitionDataStoreWriter writer7 = ctx.getBean("writer7", PartitionDataStoreWriter.class);

		Map<String, Object> partitionKey = new HashMap<String, Object>();
		partitionKey.put("timestamp", 0);
		writer7.write("foo", partitionKey);

		partitionKey.put("timestamp", 1000);
		writer7.write("foo", partitionKey);

		ctx.close();
		// rename should happen with context close

		FsShell shell = new FsShell(getConfiguration());
		Collection<FileStatus> files = shell.ls(true, Config7.DIR);
		Collection<String> names = statusesToNames(files);
		assertThat(files.size(), is(5));
		assertThat(
				names,
				containsInAnyOrder(Config7.DIR, Config7.DIR + "/0", Config7.DIR + "/0/data-0.txt", Config7.DIR + "/1000",
						Config7.DIR + "/1000/data-0.txt"));
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	@Test
	public void testPartitionTextFileWriterWriterAfterContextClose() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config8.class);
		ctx.refresh();
		PartitionDataStoreWriter writer8 = ctx.getBean("writer8", PartitionDataStoreWriter.class);

		Map<String, Object> partitionKey = new HashMap<String, Object>();
		partitionKey.put("timestamp", 0);
		writer8.write("foo", partitionKey);

		partitionKey.put("timestamp", 1000);
		writer8.write("foo", partitionKey);

		ctx.close();

		Exception expected = null;
		try {
			partitionKey.put("timestamp", 2000);
			writer8.write("foo", partitionKey);
		} catch (Exception e) {
			expected = e;
		}
		assertThat(expected, instanceOf(StoreException.class));

		// rename should happen with context close

		FsShell shell = new FsShell(getConfiguration());
		Collection<FileStatus> files = shell.ls(true, Config8.DIR);
		Collection<String> names = statusesToNames(files);
		TestUtils.printLsR(Config8.DIR, getConfiguration());
		assertThat(files.size(), is(5));
		assertThat(
				names,
				containsInAnyOrder(Config8.DIR, Config8.DIR + "/0", Config8.DIR + "/0/data-0.txt", Config8.DIR + "/1000",
						Config8.DIR + "/1000/data-0.txt"));
	}

	@Configuration
	@EnableDataStoreTextWriter(name="writer1")
	static class Config1 extends SpringDataStoreTextWriterConfigurerAdapter {

		final static String PATH = "/tmp/ContextCloseWriterTests/testSingleTextFileWriterCloseManually";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Override
		public void configure(DataStoreTextWriterConfigurer config) throws Exception {
			config
				.configuration(configuration)
				.basePath("/tmp/ContextCloseWriterTests/testSingleTextFileWriterCloseManually");
		}

	}

	@Configuration
	@EnableDataStoreTextWriter(name="writer2")
	static class Config2 extends SpringDataStoreTextWriterConfigurerAdapter {

		final static String PATH = "/tmp/ContextCloseWriterTests/testSingleTextFileWriterCloseByContext";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Override
		public void configure(DataStoreTextWriterConfigurer config) throws Exception {
			config
				.configuration(configuration)
				.basePath(PATH);
		}

	}

	@Configuration
	@EnableDataStoreTextWriter(name="writer3")
	static class Config3 extends SpringDataStoreTextWriterConfigurerAdapter {

		final static String PATH = "/tmp/ContextCloseWriterTests/testSingleTextFileWriterInUsePostfixCloseByContext/data";
		final static String PATHINUSE = "/tmp/ContextCloseWriterTests/testSingleTextFileWriterInUsePostfixCloseByContext/data.tmp";
		final static String DIR = "/tmp/ContextCloseWriterTests/testSingleTextFileWriterInUsePostfixCloseByContext";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Override
		public void configure(DataStoreTextWriterConfigurer config) throws Exception {
			config
				.configuration(configuration)
				.basePath(PATH)
				.inWritingSuffix(".tmp");
		}

	}

	@Configuration
	@EnableDataStoreTextWriter(name="writer4")
	static class Config4 extends SpringDataStoreTextWriterConfigurerAdapter {

		final static String PATH = "/tmp/ContextCloseWriterTests/testSinglePartitionTextFileWriterInUsePostfixCloseByContext/data";
		final static String PATHINUSE = "/tmp/ContextCloseWriterTests/testSinglePartitionTextFileWriterInUsePostfixCloseByContext/data.tmp";
		final static String DIR = "/tmp/ContextCloseWriterTests/testSinglePartitionTextFileWriterInUsePostfixCloseByContext";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Override
		public void configure(DataStoreTextWriterConfigurer config) throws Exception {
			config
				.configuration(configuration)
				.basePath(DIR)
				.inWritingSuffix(".tmp")
				.withNamingStrategy()
					.name("data")
					.and()
				.withPartitionStrategy()
					.map("timestamp");
		}

	}

	@Configuration
	@EnableDataStoreTextWriter(name="writer5")
	static class Config5 extends SpringDataStoreTextWriterConfigurerAdapter {

		final static String DIR = "/tmp/ContextCloseWriterTests/testSinglePartitionTextFileWriterComplexNamingCloseByContext";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Override
		public void configure(DataStoreTextWriterConfigurer config) throws Exception {
			config
				.configuration(configuration)
				.basePath(DIR)
				.idleTimeout(1000)
				.inWritingSuffix(".tmp")
				.withNamingStrategy()
					.name("data")
					.rolling()
					.name("txt", ".")
					.and()
				.withPartitionStrategy()
					.map("timestamp");
		}

		@Bean
		public TaskScheduler taskScheduler() {
			return new ConcurrentTaskScheduler();
		}

		@Bean
		public TaskExecutor taskExecutor() {
			return new ConcurrentTaskExecutor();
		}

	}

	@Configuration
	@EnableDataStoreTextWriter(name="writer6")
	static class Config6 extends SpringDataStoreTextWriterConfigurerAdapter {

		final static String DIR = "/tmp/ContextCloseWriterTests/testSinglePartitionTextFileWriterComplexNamingCloseByTimeout";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Override
		public void configure(DataStoreTextWriterConfigurer config) throws Exception {
			config
				.configuration(configuration)
				.basePath(DIR)
				.idleTimeout(1000)
				.inWritingSuffix(".tmp")
				.withNamingStrategy()
					.name("data")
					.rolling()
					.name("txt", ".")
					.and()
				.withPartitionStrategy()
					.map("timestamp");
		}

		@Bean
		public TaskScheduler taskScheduler() {
			return new ConcurrentTaskScheduler();
		}

		@Bean
		public TaskExecutor taskExecutor() {
			return new ConcurrentTaskExecutor();
		}

	}

	@Configuration
	@EnableDataStoreTextWriter(name="writer7")
	static class Config7 extends SpringDataStoreTextWriterConfigurerAdapter {

		final static String DIR = "/tmp/ContextCloseWriterTests/testPartitionTextFileWriterPrefixRemoved";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Override
		public void configure(DataStoreTextWriterConfigurer config) throws Exception {
			config
				.configuration(configuration)
				.basePath(DIR)
				.idleTimeout(60000)
				.inWritingSuffix(".tmp")
				.withNamingStrategy()
					.name("data")
					.rolling()
					.name("txt", ".")
					.and()
				.withPartitionStrategy()
					.map("timestamp");
		}

		@Bean
		public TaskScheduler taskScheduler() {
			return new ConcurrentTaskScheduler();
		}

		@Bean
		public TaskExecutor taskExecutor() {
			return new ConcurrentTaskExecutor();
		}

	}

	@Configuration
	@EnableDataStoreTextWriter(name="writer8")
	static class Config8 extends SpringDataStoreTextWriterConfigurerAdapter {

		final static String DIR = "/tmp/ContextCloseWriterTests/testPartitionTextFileWriterWriterAfterContextClose";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Override
		public void configure(DataStoreTextWriterConfigurer config) throws Exception {
			config
				.configuration(configuration)
				.basePath(DIR)
				.idleTimeout(60000)
				.inWritingSuffix(".tmp")
				.withNamingStrategy()
					.name("data")
					.rolling()
					.name("txt", ".")
					.and()
				.withPartitionStrategy()
					.map("timestamp");
		}

		@Bean
		public TaskScheduler taskScheduler() {
			return new ConcurrentTaskScheduler();
		}

		@Bean
		public TaskExecutor taskExecutor() {
			return new ConcurrentTaskExecutor();
		}

	}

	@Configuration
	static class EmptyConfig {
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

}
