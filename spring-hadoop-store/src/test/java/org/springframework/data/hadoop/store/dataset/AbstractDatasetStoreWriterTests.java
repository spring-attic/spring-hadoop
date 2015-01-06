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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractHadoopClusterTests;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class, locations={"/org/springframework/data/hadoop/store/dataset/DatasetStoreWriterTests-context.xml"})
@MiniHadoopCluster
public abstract class AbstractDatasetStoreWriterTests<T extends Comparable<T>> extends AbstractHadoopClusterTests {

	protected DatasetOperations datasetOperations;
	protected DataStoreWriter<T> datasetStoreWriter;
	protected List<T> records = new ArrayList<T>();
	protected Class<T> recordClass;

	@Autowired
	protected String path;

	public abstract void setDatasetRepositoryFactory(DatasetRepositoryFactory datasetRepositoryFactory);

	@Before
	public abstract void setUp();

	@Test
	public void testSavePojo() throws IOException {
		for (T pojo: records) {
			datasetStoreWriter.write(pojo);
		}
		datasetStoreWriter.flush();
		datasetStoreWriter.close();

		FileSystem fs = FileSystem.get(getConfiguration());
		assertTrue("Dataset path created", fs.exists(new Path(path)));
		assertTrue("Dataset storage created",
				fs.exists(new Path(path + "/test/" + DatasetUtils.getDatasetName(recordClass))));
		assertTrue("Dataset metadata created",
				fs.exists(new Path(path + "/test/" + DatasetUtils.getDatasetName(recordClass) + "/.metadata")));
		Collection<T> results = datasetOperations.read(recordClass);
		assertEquals(2, results.size());
		List<T> sorted = new ArrayList<T>(results);
		Collections.sort(sorted);
		BeanWrapper result = new BeanWrapperImpl(sorted.get(0));
		assertTrue(result.isReadableProperty("name"));
		assertTrue(result.getPropertyValue("name").equals("Sven"));
		assertTrue(result.isReadableProperty("id"));
		assertTrue(result.getPropertyValue("id").equals(22L));
		result = new BeanWrapperImpl(sorted.get(1));
		assertTrue(result.isReadableProperty("name"));
		assertTrue(result.getPropertyValue("name").equals("Nisse"));
		assertTrue(result.isReadableProperty("id"));
		assertTrue(result.getPropertyValue("id").equals(48L));
	}

}
