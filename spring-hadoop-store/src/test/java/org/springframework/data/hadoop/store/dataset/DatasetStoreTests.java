/*
 * Copyright 2014-2015 the original author or authors.
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
package org.springframework.data.hadoop.store.dataset;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.hadoop.store.DataStoreReader;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.TestUtils;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class, classes = DatasetStoreTests.EmptyConfig.class)
@MiniHadoopCluster
public class DatasetStoreTests {

	@Autowired
	private ApplicationContext context;

	@Autowired
	org.apache.hadoop.conf.Configuration configuration;

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAvroDataset() throws IOException {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config1.class);
		ctx.refresh();

		DataStoreWriter<DatasetPojo> writer = ctx.getBean(DataStoreWriter.class);
		DataStoreReader<DatasetPojo> reader = ctx.getBean(DataStoreReader.class);

		writer.write(new DatasetPojo(10, "testname10"));
		writer.close();

		writer.write(new DatasetPojo(11, "testname11"));
		writer.close();

		ArrayList<DatasetPojo> results = new ArrayList<DatasetPojo>();
		for (DatasetPojo pojo = reader.read(); pojo != null; pojo = reader.read()) {
			results.add(pojo);
		}
		Collections.sort(results);

		assertThat(results.get(0).getAge(), is(10));
		assertThat(results.get(0).getName(), is("testname10"));

		assertThat(results.get(1).getAge(), is(11));
		assertThat(results.get(1).getName(), is("testname11"));

		assertThat(reader.read(), nullValue());

		ctx.close();
		TestUtils.printLsR(Config1.PATH, configuration);
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testParquetDataset() throws IOException {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config2.class);
		ctx.refresh();

		DataStoreWriter<DatasetPojo> writer = ctx.getBean(DataStoreWriter.class);
		DataStoreReader<DatasetPojo> reader = ctx.getBean(DataStoreReader.class);

		writer.write(new DatasetPojo(10, "testname10"));
		writer.close();

		writer.write(new DatasetPojo(11, "testname11"));
		writer.close();

		ArrayList<DatasetPojo> results = new ArrayList<DatasetPojo>();
		for (DatasetPojo pojo = reader.read(); pojo != null; pojo = reader.read()) {
			results.add(pojo);
		}
		Collections.sort(results);

		assertThat(results.get(0).getAge(), is(10));
		assertThat(results.get(0).getName(), is("testname10"));

		assertThat(results.get(1).getAge(), is(11));
		assertThat(results.get(1).getName(), is("testname11"));

		assertThat(reader.read(), nullValue());

		ctx.close();
		TestUtils.printLsR(Config2.PATH, configuration);
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testIdleTimeout() throws IOException, InterruptedException {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config3.class);
		ctx.refresh();

		DataStoreWriter<DatasetPojo> writer = ctx.getBean(DataStoreWriter.class);
		DataStoreReader<DatasetPojo> reader = ctx.getBean(DataStoreReader.class);

		writer.write(new DatasetPojo(10, "testname10"));
		writer.write(new DatasetPojo(11, "testname11"));

		Thread.sleep(2000);

		ArrayList<DatasetPojo> results = new ArrayList<DatasetPojo>();
		for (DatasetPojo pojo = reader.read(); pojo != null; pojo = reader.read()) {
			results.add(pojo);
		}
		Collections.sort(results);

		assertThat(results.get(0).getAge(), is(10));
		assertThat(results.get(0).getName(), is("testname10"));

		assertThat(results.get(1).getAge(), is(11));
		assertThat(results.get(1).getName(), is("testname11"));

		assertThat(reader.read(), nullValue());

		ctx.close();
		TestUtils.printLsR(Config3.PATH, configuration);
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testContextClose() throws IOException {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config4.class);
		ctx.refresh();

		DataStoreWriter<DatasetPojo> writer = ctx.getBean(DataStoreWriter.class);

		writer.write(new DatasetPojo(10, "testname10"));
		writer.close();

		writer.write(new DatasetPojo(11, "testname11"));
		writer.close();

		writer.write(new DatasetPojo(12, "testname12"));
		// close ctx used for writing without closing a writer
		ctx.close();

		// open new ctx for reader
		ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config4.class);
		ctx.refresh();

		DataStoreReader<DatasetPojo> reader = ctx.getBean(DataStoreReader.class);
		ArrayList<DatasetPojo> results = new ArrayList<DatasetPojo>();
		for (DatasetPojo pojo = reader.read(); pojo != null; pojo = reader.read()) {
			results.add(pojo);
		}
		Collections.sort(results);

		assertThat(results.get(0).getAge(), is(10));
		assertThat(results.get(0).getName(), is("testname10"));

		assertThat(results.get(1).getAge(), is(11));
		assertThat(results.get(1).getName(), is("testname11"));

		assertThat(results.get(2).getAge(), is(12));
		assertThat(results.get(2).getName(), is("testname12"));

		assertThat(reader.read(), nullValue());

		ctx.close();
		TestUtils.printLsR(Config4.PATH, configuration);
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testCloseTimeout() throws IOException, InterruptedException {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config5.class);
		ctx.refresh();

		DataStoreWriter<DatasetPojo> writer = ctx.getBean(DataStoreWriter.class);
		DataStoreReader<DatasetPojo> reader = ctx.getBean(DataStoreReader.class);

		writer.write(new DatasetPojo(10, "testname10"));
		writer.write(new DatasetPojo(11, "testname11"));

		Thread.sleep(2000);

		ArrayList<DatasetPojo> results = new ArrayList<DatasetPojo>();
		for (DatasetPojo pojo = reader.read(); pojo != null; pojo = reader.read()) {
			results.add(pojo);
		}
		Collections.sort(results);

		assertThat(results.get(0).getAge(), is(10));
		assertThat(results.get(0).getName(), is("testname10"));

		assertThat(results.get(1).getAge(), is(11));
		assertThat(results.get(1).getName(), is("testname11"));

		assertThat(reader.read(), nullValue());

		ctx.close();
		TestUtils.printLsR(Config5.PATH, configuration);
	}

	@Configuration
	static class Config1 {

		final static String PATH = "/tmp/DatasetStoreTests/Config1";
		final static String NAMESPACE = "test";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Bean
		public DatasetRepositoryFactory datasetRepositoryFactory() {
			DatasetRepositoryFactory factory = new DatasetRepositoryFactory();
			factory.setConf(configuration);
			factory.setBasePath(PATH);
			factory.setNamespace(NAMESPACE);
			return factory;
		}

		@Bean
		public DatasetDefinition datasetDefinition() {
			DatasetDefinition definition = new DatasetDefinition();
			return definition;
		}

		@Bean
		public DataStoreWriter<DatasetPojo> dataStoreWriter() {
			AvroPojoDatasetStoreWriter<DatasetPojo> writer = new AvroPojoDatasetStoreWriter<DatasetPojo>(
					DatasetPojo.class, datasetRepositoryFactory(), datasetDefinition());
			return writer;
		}

		@Bean
		public DataStoreReader<DatasetPojo> dataStoreReader() {
			AvroPojoDatasetStoreReader<DatasetPojo> reader = new AvroPojoDatasetStoreReader<DatasetPojo>(
					DatasetPojo.class, datasetRepositoryFactory(), datasetDefinition());
			return reader;
		}

	}

	@Configuration
	static class Config2 {

		final static String PATH = "/tmp/DatasetStoreTests/Config2";
		final static String NAMESPACE = "test";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Bean
		public DatasetRepositoryFactory datasetRepositoryFactory() {
			DatasetRepositoryFactory factory = new DatasetRepositoryFactory();
			factory.setConf(configuration);
			factory.setBasePath(PATH);
			factory.setNamespace(NAMESPACE);
			return factory;
		}

		@Bean
		public DatasetDefinition datasetDefinition() {
			DatasetDefinition definition = new DatasetDefinition();
			definition.setFormat("parquet");
			return definition;
		}

		@Bean
		public DataStoreWriter<DatasetPojo> dataStoreWriter() {
			ParquetDatasetStoreWriter<DatasetPojo> writer = new ParquetDatasetStoreWriter<DatasetPojo>(
					DatasetPojo.class, datasetRepositoryFactory(), datasetDefinition());
			return writer;
		}

		@Bean
		public DataStoreReader<DatasetPojo> dataStoreReader() {
			ParquetDatasetStoreReader<DatasetPojo> reader = new ParquetDatasetStoreReader<DatasetPojo>(
					DatasetPojo.class, datasetRepositoryFactory(), datasetDefinition());
			return reader;
		}

	}

	@Configuration
	static class Config3 {

		final static String PATH = "/tmp/DatasetStoreTests/Config3";
		final static String NAMESPACE = "test";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Bean
		public DatasetRepositoryFactory datasetRepositoryFactory() {
			DatasetRepositoryFactory factory = new DatasetRepositoryFactory();
			factory.setConf(configuration);
			factory.setBasePath(PATH);
			factory.setNamespace(NAMESPACE);
			return factory;
		}

		@Bean
		public DatasetDefinition datasetDefinition() {
			DatasetDefinition definition = new DatasetDefinition();
			return definition;
		}

		@Bean
		public DataStoreWriter<DatasetPojo> dataStoreWriter() {
			AvroPojoDatasetStoreWriter<DatasetPojo> writer = new AvroPojoDatasetStoreWriter<DatasetPojo>(
					DatasetPojo.class, datasetRepositoryFactory(), datasetDefinition());
			writer.setIdleTimeout(500);
			return writer;
		}

		@Bean
		public DataStoreReader<DatasetPojo> dataStoreReader() {
			AvroPojoDatasetStoreReader<DatasetPojo> reader = new AvroPojoDatasetStoreReader<DatasetPojo>(
					DatasetPojo.class, datasetRepositoryFactory(), datasetDefinition());
			return reader;
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
	static class Config4 {

		final static String PATH = "/tmp/DatasetStoreTests/Config4";
		final static String NAMESPACE = "test";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Bean
		public DatasetRepositoryFactory datasetRepositoryFactory() {
			DatasetRepositoryFactory factory = new DatasetRepositoryFactory();
			factory.setConf(configuration);
			factory.setBasePath(PATH);
			factory.setNamespace(NAMESPACE);
			return factory;
		}

		@Bean
		public DatasetDefinition datasetDefinition() {
			DatasetDefinition definition = new DatasetDefinition();
			return definition;
		}

		@Bean
		public DataStoreWriter<DatasetPojo> dataStoreWriter() {
			AvroPojoDatasetStoreWriter<DatasetPojo> writer = new AvroPojoDatasetStoreWriter<DatasetPojo>(
					DatasetPojo.class, datasetRepositoryFactory(), datasetDefinition());
			return writer;
		}

		@Bean
		public DataStoreReader<DatasetPojo> dataStoreReader() {
			AvroPojoDatasetStoreReader<DatasetPojo> reader = new AvroPojoDatasetStoreReader<DatasetPojo>(
					DatasetPojo.class, datasetRepositoryFactory(), datasetDefinition());
			return reader;
		}

	}

	@Configuration
	static class Config5 {

		final static String PATH = "/tmp/DatasetStoreTests/Config5";
		final static String NAMESPACE = "test";

		@Autowired
		org.apache.hadoop.conf.Configuration configuration;

		@Bean
		public DatasetRepositoryFactory datasetRepositoryFactory() {
			DatasetRepositoryFactory factory = new DatasetRepositoryFactory();
			factory.setConf(configuration);
			factory.setBasePath(PATH);
			factory.setNamespace(NAMESPACE);
			return factory;
		}

		@Bean
		public DatasetDefinition datasetDefinition() {
			DatasetDefinition definition = new DatasetDefinition();
			return definition;
		}

		@Bean
		public DataStoreWriter<DatasetPojo> dataStoreWriter() {
			AvroPojoDatasetStoreWriter<DatasetPojo> writer = new AvroPojoDatasetStoreWriter<DatasetPojo>(
					DatasetPojo.class, datasetRepositoryFactory(), datasetDefinition());
			writer.setCloseTimeout(500);
			return writer;
		}

		@Bean
		public DataStoreReader<DatasetPojo> dataStoreReader() {
			AvroPojoDatasetStoreReader<DatasetPojo> reader = new AvroPojoDatasetStoreReader<DatasetPojo>(
					DatasetPojo.class, datasetRepositoryFactory(), datasetDefinition());
			return reader;
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

	public static class DatasetPojo implements Comparable<DatasetPojo>{
		Integer age;
		String name;
		public DatasetPojo() {
		}
		public DatasetPojo(Integer age, String name) {
			this.age = age;
			this.name = name;
		}
		public Integer getAge() {
			return age;
		}
		public void setAge(Integer age) {
			this.age = age;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		@Override
		public int compareTo(DatasetPojo o) {
			return age.compareTo(o.getAge());
		}
	}

}
