/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.hadoop.store.config.annotation.EnableDataStorePartitionTextWriter;
import org.springframework.data.hadoop.store.config.annotation.EnableDataStoreTextWriter;
import org.springframework.data.hadoop.store.config.annotation.SpringDataStoreTextWriterConfigurerAdapter;
import org.springframework.data.hadoop.store.config.annotation.builders.DataStoreTextWriterConfigurer;
import org.springframework.data.hadoop.store.input.TextFileReader;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class, classes = TimeoutTests.EmptyConfig.class)
@MiniHadoopCluster
public class TimeoutTests {

	@Autowired
	private ApplicationContext context;

	@Autowired
	org.apache.hadoop.conf.Configuration configuration;

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testIdleTimeout() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config.class, Config1.class);
		ctx.refresh();

		DataStoreWriter<String> writer = ctx.getBean(DataStoreWriter.class);
		String[] dataArray = new String[] { "0123456789" };
		TestUtils.writeData(writer, dataArray, false, false);

		Thread.sleep(2000);

		TextFileReader reader = new TextFileReader(configuration, new Path("/tmp/TimeoutTests/testIdleTimeout/data"), null);
		TestUtils.readDataAndAssert(reader, dataArray);

		ctx.close();
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testCloseTimeout() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config.class, Config2.class);
		ctx.refresh();

		DataStoreWriter<String> writer = ctx.getBean(DataStoreWriter.class);
		String[] dataArray = new String[] { "0123456789" };
		TestUtils.writeData(writer, dataArray, false, false);

		Thread.sleep(2000);

		TextFileReader reader = new TextFileReader(configuration, new Path("/tmp/TimeoutTests/testCloseTimeout/data"), null);
		TestUtils.readDataAndAssert(reader, dataArray);

		ctx.close();
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testCloseTimeoutWithPartitioning() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config.class, Config3.class);
		ctx.refresh();

		DataStoreWriter<String> writer = ctx.getBean(DataStoreWriter.class);
		String[] dataArray1 = new String[] { "fin", "swe" };
		String[] dataArray2 = new String[] { "eng", "sco" };
		TestUtils.writeData(writer, dataArray1, false, false);
		TestUtils.writeData(writer, dataArray2, false, false);

		Thread.sleep(2000);

		TextFileReader reader = new TextFileReader(configuration, new Path("/tmp/TimeoutTests/testCloseTimeoutWithPartitioning/data/nordic_list"), null);
		TestUtils.readDataAndAssert(reader, dataArray1);
		reader = new TextFileReader(configuration, new Path("/tmp/TimeoutTests/testCloseTimeoutWithPartitioning/data/britain_list"), null);
		TestUtils.readDataAndAssert(reader, dataArray2);

		ctx.close();
	}

	@Configuration
	static class Config {

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
	@EnableDataStoreTextWriter
	static class Config1 extends SpringDataStoreTextWriterConfigurerAdapter {

		@Override
		public void configure(DataStoreTextWriterConfigurer config) throws Exception {
			config
				.basePath("/tmp/TimeoutTests/testIdleTimeout/data")
				.idleTimeout(1000);
		}

	}

	@Configuration
	@EnableDataStoreTextWriter
	static class Config2 extends SpringDataStoreTextWriterConfigurerAdapter {

		@Override
		public void configure(DataStoreTextWriterConfigurer config) throws Exception {
			config
				.basePath("/tmp/TimeoutTests/testCloseTimeout/data")
				.closeTimeout(1000);
		}

	}

	@Configuration
	@EnableDataStorePartitionTextWriter
	static class Config3 extends SpringDataStoreTextWriterConfigurerAdapter {

		@Override
		public void configure(DataStoreTextWriterConfigurer config) throws Exception {
			config
				.basePath("/tmp/TimeoutTests/testCloseTimeoutWithPartitioning/data")
				.closeTimeout(1000)
				.withPartitionStrategy()
					.map("list(content,{{'nordic','fin','swe'},{'britain','eng','sco'}})");
		}

	}

	@Configuration
	static class EmptyConfig {
	}

}
