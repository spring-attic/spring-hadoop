package org.springframework.data.hadoop.store.dataset;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.DatasetRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.test.tests.Assume;
import org.springframework.data.hadoop.test.tests.Version;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractDatasetTemplateTests {

	protected DatasetOperations datasetOperations;
	protected List<Object> records = new ArrayList<Object>();
	@Autowired
	protected String path;

	@Autowired
	public void setDatasetOperations(DatasetOperations datasetOperations) {
		this.datasetOperations = datasetOperations;
	}

	@Test
	public void testSavePojo() {
		datasetOperations.write(records);
		assertTrue("Dataset path created", new File(path).exists());
		assertTrue("Dataset storage created",
				new File(path + "/" + datasetOperations.getDatasetName(TestPojo.class)).exists());
		assertTrue("Dataset metadata created",
				new File(path + "/" + datasetOperations.getDatasetName(TestPojo.class) + "/.metadata").exists());
	}

	@Test
	public void testReadSavedPojoWithCallback() {
		datasetOperations.write(records);
		final List<TestPojo> results = new ArrayList<TestPojo>();
		datasetOperations.read(TestPojo.class, new RecordCallback<TestPojo>() {

			@Override
			public void doInRecord(TestPojo record) {
				results.add(record);
			}
		});
		assertEquals(2, results.size());
		if (results.get(0).getId().equals(22L)) {
			assertTrue(results.get(0).getName().equals("Sven"));
			assertTrue(results.get(1).getName().equals("Nisse"));
		}
		else {
			assertTrue(results.get(0).getName().equals("Nisse"));
			assertTrue(results.get(1).getName().equals("Sven"));
		}
	}

	@Test
	public void testReadSavedPojoCollection() {
		datasetOperations.write(records);
		TestPojo pojo3 = new TestPojo();
		pojo3.setId(31L);
		pojo3.setName("Eric");
		pojo3.setBirthDate(new Date());
		datasetOperations.write(Collections.singletonList(pojo3));
		Collection<TestPojo> results = datasetOperations.read(TestPojo.class);
		assertEquals(3, results.size());
		List<TestPojo> sorted = new ArrayList<TestPojo>(results);
		Collections.sort(sorted);
		assertTrue(sorted.get(0).getName().equals("Sven"));
		assertTrue(sorted.get(0).getId().equals(22L));
		assertTrue(sorted.get(1).getName().equals("Eric"));
		assertTrue(sorted.get(1).getId().equals(31L));
		assertTrue(sorted.get(2).getName().equals("Nisse"));
		assertTrue(sorted.get(2).getId().equals(48L));
	}

}
