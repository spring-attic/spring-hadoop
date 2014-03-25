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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.test.tests.Assume;
import org.springframework.data.hadoop.test.tests.Version;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import org.kitesdk.data.DatasetRepository;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class DatasetTemplateTests extends AbstractDatasetTemplateTests {

	@Before
	public void setUp() {
		TestPojo pojo1 = new TestPojo();
		pojo1.setId(22L);
		pojo1.setName("Sven");
		pojo1.setBirthDate(new Date());
		records.add(pojo1);
		TestPojo pojo2 = new TestPojo();
		pojo2.setId(48L);
		pojo2.setName("Nisse");
		pojo2.setBirthDate(new Date());
		records.add(pojo2);

		datasetOperations.execute(new DatasetRepositoryCallback() {
			@Override
			public void doInRepository(DatasetRepository datasetRepository) {
				datasetRepository.delete(datasetOperations.getDatasetName(TestPojo.class));
				datasetRepository.delete(datasetOperations.getDatasetName(AnotherPojo.class));
			}
		});
	}

	@Test
	public void testReadSavedPojoWithNullValues() {
		datasetOperations.write(records);
		TestPojo pojo4 = new TestPojo();
		pojo4.setId(33L);
		pojo4.setName(null);
		pojo4.setBirthDate(null);
		datasetOperations.write(Collections.singletonList(pojo4));
		Collection<TestPojo> results = datasetOperations.read(TestPojo.class);
		assertEquals(3, results.size());
		List<TestPojo> sorted = new ArrayList<TestPojo>(results);
		Collections.sort(sorted);
		assertTrue(sorted.get(0).getName().equals("Sven"));
		assertTrue(sorted.get(0).getId().equals(22L));
		assertNull(sorted.get(1).getName());
		assertTrue(sorted.get(1).getId().equals(33L));
		assertTrue(sorted.get(2).getName().equals("Nisse"));
		assertTrue(sorted.get(2).getId().equals(48L));
	}

	@Test
	public void testSaveAndReadMultiplePojoClasses() {
		List<AnotherPojo> others = new ArrayList<AnotherPojo>();
		AnotherPojo other1 = new AnotherPojo();
		other1.setId(111L);
		other1.setDescription("This is another pojo #1");
		others.add(other1);
		AnotherPojo other2 = new AnotherPojo();
		other2.setId(222L);
		other2.setDescription("This is another pojo #2");
		others.add(other2);
		AnotherPojo other3 = new AnotherPojo();
		other3.setId(333L);
		other3.setDescription("This is another pojo #3");
		others.add(other3);
		datasetOperations.write(others);
		datasetOperations.write(records);
		assertTrue("Dataset storage created for AnotherPojo",
				new File(path + "/" + datasetOperations.getDatasetName(AnotherPojo.class)).exists());
		assertTrue("Dataset metadata created for AnotherPojo",
				new File(path + "/" + datasetOperations.getDatasetName(AnotherPojo.class) + "/.metadata").exists());
		assertTrue("Dataset storage created for TestPojo",
				new File(path + "/" + datasetOperations.getDatasetName(TestPojo.class)).exists());
		assertTrue("Dataset metadata created for TestPojo",
				new File(path + "/" + datasetOperations.getDatasetName(TestPojo.class) + "/.metadata").exists());
		Collection<AnotherPojo> otherPojos = datasetOperations.read(AnotherPojo.class);
		assertEquals(3, otherPojos.size());
		List<AnotherPojo> sorted = new ArrayList<AnotherPojo>(otherPojos);
		Collections.sort(sorted);
		assertTrue(sorted.get(0).getDescription().equals(other1.getDescription()));
		assertTrue(sorted.get(0).getId().equals(111L));
		assertTrue(sorted.get(1).getDescription().equals(other2.getDescription()));
		assertTrue(sorted.get(1).getId().equals(222L));
		assertTrue(sorted.get(2).getDescription().equals(other3.getDescription()));
		assertTrue(sorted.get(2).getId().equals(333L));
		Collection<TestPojo> testPojos = datasetOperations.read(TestPojo.class);
		assertEquals(2, testPojos.size());
	}

}
