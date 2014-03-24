package org.springframework.data.hadoop.store.dataset;

import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.DatasetRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.test.tests.Assume;
import org.springframework.data.hadoop.test.tests.Version;

import java.io.File;
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
	protected List<TestPojo> records = new ArrayList<TestPojo>();
	@Autowired
	private String path;

	@Autowired
	public void setDatasetOperations(DatasetOperations datasetOperations) {
		this.datasetOperations = datasetOperations;
	}

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
		//CDK currently uses 2.0 only org.apache.hadoop.fs.FileStatus.isDirectory()
		Assume.hadoopVersion(Version.HADOOP2X);
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
		//Kite SDK currently uses some Hadoop 2.0 only methods
		Assume.hadoopVersion(Version.HADOOP2X);
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

	@Test
	public void testSaveAndReadMultiplePojoClasses() {
		//Kite SDK currently uses some Hadoop 2.0 only methods
		Assume.hadoopVersion(Version.HADOOP2X);
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
