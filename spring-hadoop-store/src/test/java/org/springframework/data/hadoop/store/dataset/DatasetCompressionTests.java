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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.Dataset;
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
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class, classes = DatasetCompressionTests.EmptyConfig.class)
@MiniHadoopCluster
public class DatasetCompressionTests {

	@Autowired
	private ApplicationContext context;

	@Autowired
	org.apache.hadoop.conf.Configuration configuration;

	@Test
	public void testDefaultAvroCodecIsSnappy() throws IOException {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(AvroSnappyConfig.class);
		ctx.refresh();

		DatasetRepositoryFactory factory = ctx.getBean(DatasetRepositoryFactory.class);
		AvroPojoDatasetStoreWriter<TestPojo> writer = new AvroPojoDatasetStoreWriter<TestPojo>(TestPojo.class, factory);

		DatasetOperations datasetOperations = new DatasetTemplate(factory);
		Class<TestPojo> recordClass = TestPojo.class;

		TestPojo pojo = new TestPojo();
		pojo.setId(22L);
		pojo.setName("Sven");
		pojo.setBirthDate(new Date());
		doAssertSimpleWrite(writer, datasetOperations, recordClass, AvroSnappyConfig.PATH, AvroSnappyConfig.NAMESPACE, pojo);

		Dataset<TestPojo> dataset = DatasetUtils.getDataset(factory, recordClass);
		assertThat(dataset.getDescriptor().getCompressionType(), is(CompressionType.Snappy));

		ctx.close();
	}

	@Test
	public void testAvroBzip2Codec() throws IOException {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(AvroBzip2CodecConfig.class);
		ctx.refresh();

		DatasetRepositoryFactory factory = ctx.getBean(DatasetRepositoryFactory.class);
		DatasetDefinition definition = ctx.getBean(DatasetDefinition.class);
		AvroPojoDatasetStoreWriter<TestPojo> writer = new AvroPojoDatasetStoreWriter<TestPojo>(TestPojo.class, factory, definition);

		DatasetOperations datasetOperations = new DatasetTemplate(factory);
		Class<TestPojo> recordClass = TestPojo.class;

		TestPojo pojo = new TestPojo();
		pojo.setId(22L);
		pojo.setName("Sven");
		pojo.setBirthDate(new Date());
		doAssertSimpleWrite(writer, datasetOperations, recordClass, AvroBzip2CodecConfig.PATH, AvroBzip2CodecConfig.NAMESPACE, pojo);

		Dataset<TestPojo> dataset = DatasetUtils.getDataset(factory, recordClass);
		assertThat(dataset.getDescriptor().getCompressionType(), is(CompressionType.Bzip2));

		ctx.close();
	}

	@Test
	public void testDefaultParquetCodecIsSnappy() throws IOException {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(ParquetSnappyConfig.class);
		ctx.refresh();

		DatasetRepositoryFactory factory = ctx.getBean(DatasetRepositoryFactory.class);
		ParquetDatasetStoreWriter<SimplePojo> writer = new ParquetDatasetStoreWriter<SimplePojo>(SimplePojo.class, factory);

		DatasetOperations datasetOperations = new DatasetTemplate(factory);
		Class<SimplePojo> recordClass = SimplePojo.class;

		SimplePojo pojo = new SimplePojo();
		pojo.setId(22L);
		pojo.setName("Sven");
		pojo.setBirthDate(new Date().getTime());
		doAssertSimpleWrite(writer, datasetOperations, recordClass, ParquetSnappyConfig.PATH, ParquetSnappyConfig.NAMESPACE, pojo);

		Dataset<SimplePojo> dataset = DatasetUtils.getDataset(factory, recordClass);
		assertThat(dataset.getDescriptor().getCompressionType(), is(CompressionType.Snappy));

		ctx.close();
	}

	@Test
	public void testParquetDeflateCodec() throws IOException {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.setParent(context);
		ctx.register(ParquetDeflateCodecConfig.class);
		ctx.refresh();

		DatasetRepositoryFactory factory = ctx.getBean(DatasetRepositoryFactory.class);
		DatasetDefinition definition = ctx.getBean(DatasetDefinition.class);
		ParquetDatasetStoreWriter<SimplePojo> writer = new ParquetDatasetStoreWriter<SimplePojo>(SimplePojo.class, factory, definition);

		DatasetOperations datasetOperations = new DatasetTemplate(factory);
		Class<SimplePojo> recordClass = SimplePojo.class;

		SimplePojo pojo = new SimplePojo();
		pojo.setId(22L);
		pojo.setName("Sven");
		pojo.setBirthDate(new Date().getTime());
		doAssertSimpleWrite(writer, datasetOperations, recordClass, ParquetDeflateCodecConfig.PATH, ParquetDeflateCodecConfig.NAMESPACE, pojo);

		Dataset<SimplePojo> dataset = DatasetUtils.getDataset(factory, recordClass);
		assertThat(dataset.getDescriptor().getCompressionType(), is(CompressionType.Deflate));

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
	static class AvroSnappyConfig {

		final static String PATH = "/tmp/DatasetCompressionTests/testDefaultAvroCodecIsSnappy";
		final static String NAMESPACE = "testDefaultAvroCodecIsSnappy";

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
	static class AvroBzip2CodecConfig {

		final static String PATH = "/tmp/DatasetCompressionTests/testAvroBzip2Codec";
		final static String NAMESPACE = "testAvroBzip2Codec";

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
			template.setDefaultDatasetDefinition(datasetDefinition());
			return template;
		}

		@Bean
		public DatasetDefinition datasetDefinition() {
			DatasetDefinition definition = new DatasetDefinition();
			definition.setCompressionType("bzip2");
			return definition;
		}

	}

	@Configuration
	static class ParquetSnappyConfig {

		final static String PATH = "/tmp/DatasetCompressionTests/testDefaultParquetCodecIsSnappy";
		final static String NAMESPACE = "testDefaultParquetCodecIsSnappy";

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
	static class ParquetDeflateCodecConfig {

		final static String PATH = "/tmp/DatasetCompressionTests/testParquetDeflateCodec";
		final static String NAMESPACE = "testParquetDeflateCodec";

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
			template.setDefaultDatasetDefinition(datasetDefinition());
			return template;
		}

		@Bean
		public DatasetDefinition datasetDefinition() {
			DatasetDefinition definition = new DatasetDefinition(false, "parquet");
			definition.setCompressionType("deflate");
			return definition;
		}

	}

	@Configuration
	static class EmptyConfig {
	}

}
