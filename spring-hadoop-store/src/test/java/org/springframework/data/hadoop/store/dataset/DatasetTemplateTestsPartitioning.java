/*
 * Copyright 2013 the original author or authors.
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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"DatasetTemplateTests-context.xml"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class DatasetTemplateTestsPartitioning {

	protected DatasetOperations datasetOperations;
	protected List<Object> records = new ArrayList<Object>();
	@Autowired
	protected String path;

	@Autowired
	public void setDatasetOperations(DatasetOperations datasetOperations) {
		PartitionStrategy partitionStrategy =
				new PartitionStrategy.Builder().year("birthDate").month("birthDate").build();
		((DatasetTemplate) datasetOperations).setDatasetDefinitions(Arrays.asList(
						new DatasetDefinition[] {new DatasetDefinition(SimplePojo.class, "avro", partitionStrategy)}));
		this.datasetOperations = datasetOperations;
	}

	@Before
	public void setUp() {
		SimplePojo pojo1 = new SimplePojo();
		pojo1.setId(22L);
		pojo1.setName("Sven");
		pojo1.setBirthDate(98761000902L);
		records.add(pojo1);
		SimplePojo pojo2 = new SimplePojo();
		pojo2.setId(48L);
		pojo2.setName("Nisse");
		pojo2.setBirthDate(128761080045L);
		records.add(pojo2);

		datasetOperations.execute(new DatasetRepositoryCallback() {
			@Override
			public void doInRepository(DatasetRepository datasetRepository) {
				datasetRepository.delete(datasetOperations.getDatasetName(SimplePojo.class));
			}
		});
	}

	@Test
	public void testSavePojo() {
		datasetOperations.write(records);
		assertTrue("Dataset path created", new File(path).exists());
		assertTrue("Dataset storage created",
				new File(path + "/" + datasetOperations.getDatasetName(SimplePojo.class)).exists());
		assertTrue("Dataset metadata created",
				new File(path + "/" + datasetOperations.getDatasetName(SimplePojo.class) + "/.metadata").exists());
		Collection<SimplePojo> results = datasetOperations.read(SimplePojo.class);
		assertEquals(2, results.size());
		List<SimplePojo> sorted = new ArrayList<SimplePojo>(results);
		Collections.sort(sorted);
		assertTrue(sorted.get(0).getName().equals("Sven"));
		assertTrue(sorted.get(0).getId().equals(22L));
		assertNotNull(sorted.get(0).getBirthDate());
		assertTrue(sorted.get(1).getName().equals("Nisse"));
		assertTrue(sorted.get(1).getId().equals(48L));
		assertNotNull(sorted.get(1).getBirthDate());
	}

	@Test
	public void testReadPartition() {
		SimplePojo pojo3 = new SimplePojo();
		pojo3.setId(18L);
		pojo3.setName("Maria");
		pojo3.setBirthDate(121761080045L);
		System.out.println(new Date(pojo3.getBirthDate()));
		records.add(pojo3);
		datasetOperations.write(records);
		assertTrue("Dataset path created", new File(path).exists());
		assertTrue("Dataset storage created",
				new File(path + "/" + datasetOperations.getDatasetName(SimplePojo.class)).exists());
		assertTrue("Dataset metadata created",
				new File(path + "/" + datasetOperations.getDatasetName(SimplePojo.class) + "/.metadata").exists());
		DatasetDescriptor descriptor = datasetOperations.getDatasetDescriptor(SimplePojo.class);
		PartitionKey key1973 = descriptor.getPartitionStrategy().partitionKey("1973");
		PartitionKey key1973Nov = descriptor.getPartitionStrategy().partitionKey("1973", "11");
		Collection<SimplePojo> results1973 = datasetOperations.read(SimplePojo.class, key1973);
		assertEquals(2, results1973.size());
		Collection<SimplePojo> results1973Nov = datasetOperations.read(SimplePojo.class, key1973Nov);
		assertEquals(1, results1973Nov.size());
		List<SimplePojo> sorted = new ArrayList<SimplePojo>(results1973);
		Collections.sort(sorted);
		System.out.println(new Date(sorted.get(0).getBirthDate()));
		assertTrue(sorted.get(0).getName().equals("Maria"));
		assertTrue(sorted.get(0).getId().equals(18L));
		assertNotNull(sorted.get(0).getBirthDate());
		assertTrue(sorted.get(1).getName().equals("Sven"));
		assertTrue(sorted.get(1).getId().equals(22L));
		assertNotNull(sorted.get(1).getBirthDate());
	}

}
