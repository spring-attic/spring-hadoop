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
package org.springframework.data.hadoop.store.dataset;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class, classes = DatasetExtAvroSchemaTests.EmptyConfig.class)
@MiniHadoopCluster
public class DatasetExtAvroSchemaTests {

	public static final Schema SCHEMA_ANIMAL;
	public static final Schema SCHEMA_DOG;
	static {
		try {
			SCHEMA_ANIMAL = new Parser().parse(DatasetExtAvroSchemaTests.class
					.getResourceAsStream("/org/springframework/data/hadoop/store/dataset/animal.avsc"));
			SCHEMA_DOG = new Parser().parse(DatasetExtAvroSchemaTests.class
					.getResourceAsStream("/org/springframework/data/hadoop/store/dataset/dog.avsc"));
		} catch (IOException e) {
			throw new AvroRuntimeException(e);
		}
	}

	@Autowired
	private ApplicationContext context;

	@Autowired
	org.apache.hadoop.conf.Configuration configuration;

	@Test
	public void testWithMatchingSchema() throws IOException {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config1.class);
		ctx.refresh();

		DatasetDefinition datasetDefinition = new DatasetDefinition();
		datasetDefinition.setSchema(SCHEMA_DOG);
		DatasetRepositoryFactory factory = ctx.getBean(DatasetRepositoryFactory.class);
		AvroPojoDatasetStoreWriter<DogPojo> writer = new AvroPojoDatasetStoreWriter<DogPojo>(DogPojo.class, factory, datasetDefinition);

		DatasetOperations datasetOperations = new DatasetTemplate(factory, datasetDefinition);
		Class<DogPojo> recordClass = DogPojo.class;

		DogPojo pojo = new DogPojo("southpark", 10, "doggie");
		doAssertSimpleWrite(writer, datasetOperations, recordClass, Config1.PATH, Config1.NAMESPACE, pojo);

		ctx.close();
	}

	@Test
	public void testWithDifferentSchema() throws IOException {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(Config2.class);
		ctx.refresh();

		DatasetDefinition datasetDefinition = new DatasetDefinition();
		datasetDefinition.setSchema(SCHEMA_ANIMAL);
		DatasetRepositoryFactory factory = ctx.getBean(DatasetRepositoryFactory.class);
		AvroPojoDatasetStoreWriter<DogPojo> writer = new AvroPojoDatasetStoreWriter<DogPojo>(DogPojo.class, factory, datasetDefinition);

		DatasetOperations datasetOperations = new DatasetTemplate(factory, datasetDefinition);

		DogPojo pojo = new DogPojo("southpark", 10, "doggie");

		Class<DogPojo> recordClass = DogPojo.class;
		writer.write(pojo);
		writer.flush();
		writer.close();

		Collection<DogPojo> results = datasetOperations.read(recordClass);
		DogPojo dogPojo = results.iterator().next();
		assertThat(dogPojo, instanceOf(recordClass));
		assertThat(dogPojo.getAge(), is(10));
		assertThat(dogPojo.getName(), is("doggie"));
		assertThat(dogPojo.getPark(), nullValue());

		ctx.close();
	}

	private <T> void doAssertSimpleWrite(DataStoreWriter<T> writer, DatasetOperations datasetOperations,
			Class<T> recordClass, String path, String namespace, T pojo) throws IOException {
		writer.write(pojo);
		writer.flush();
		writer.close();
		FileSystem fs = FileSystem.get(configuration);
		assertTrue("Dataset path created", fs.exists(new Path(path)));
		assertTrue("Dataset storage created",
				fs.exists(new Path(path + "/" + namespace + "/" + DatasetUtils.getDatasetName(recordClass))));
		assertTrue("Dataset metadata created",
				fs.exists(new Path(path + "/" + namespace + "/" + DatasetUtils.getDatasetName(recordClass) + "/.metadata")));
		Collection<T> results = datasetOperations.read(recordClass);
		assertEquals(1, results.size());
		assertThat(results.iterator().next(), instanceOf(recordClass));
	}

	@Configuration
	static class Config1 {

		final static String PATH = "/tmp/DatasetExtAvroSchemaTests/testWithMatchingSchema";
		final static String NAMESPACE = "test1";

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
		public DatasetTemplate datasetTemplate() {
			DatasetTemplate template = new DatasetTemplate();
			template.setDatasetRepositoryFactory(datasetRepositoryFactory());
			return template;
		}

	}

	@Configuration
	static class Config2 {

		final static String PATH = "/tmp/DatasetExtAvroSchemaTests/testWithDifferentSchema";
		final static String NAMESPACE = "test2";

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

	}

	@Configuration
	static class EmptyConfig {
	}

	public static class AnimalPojo {
		Integer age;
		String name;
		public AnimalPojo() {
		}
		public AnimalPojo(Integer age, String name) {
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
	}

	public static class DogPojo extends AnimalPojo {
		String park;
		public DogPojo() {
		}
		public DogPojo(String park, Integer age, String name) {
			super(age, name);
			this.park = park;
		}
		public String getPark() {
			return park;
		}
		public void setPark(String park) {
			this.park = park;
		}
	}

}
