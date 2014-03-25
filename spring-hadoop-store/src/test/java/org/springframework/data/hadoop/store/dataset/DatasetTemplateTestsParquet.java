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
import org.kitesdk.data.DatasetRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"DatasetTemplateTests-context.xml"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class DatasetTemplateTestsParquet {

	protected DatasetOperations datasetOperations;
	protected List<Object> records = new ArrayList<Object>();
	@Autowired
	protected String path;

	@Autowired
	public void setDatasetOperations(DatasetOperations datasetOperations) {
		((DatasetTemplate)datasetOperations)
				.setDefaultDatasetDefinition(new DatasetDefinition(true, "parquet"));
		this.datasetOperations = datasetOperations;
	}

	@Before
	public void setUp() {
		ParquetPojo pojo1 = new ParquetPojo();
		pojo1.setId(22L);
		pojo1.setName("Sven");
		pojo1.setBirthDate(new Date().getTime());
		records.add(pojo1);
		ParquetPojo pojo2 = new ParquetPojo();
		pojo2.setId(48L);
		pojo2.setName("Nisse");
		pojo2.setBirthDate(new Date().getTime());
		records.add(pojo2);

		datasetOperations.execute(new DatasetRepositoryCallback() {
			@Override
			public void doInRepository(DatasetRepository datasetRepository) {
				datasetRepository.delete(datasetOperations.getDatasetName(ParquetPojo.class));
			}
		});
	}

	@Test
	public void testSavePojo() {
		datasetOperations.write(records);
		assertTrue("Dataset path created", new File(path).exists());
		assertTrue("Dataset storage created",
				new File(path + "/" + datasetOperations.getDatasetName(ParquetPojo.class)).exists());
		assertTrue("Dataset metadata created",
				new File(path + "/" + datasetOperations.getDatasetName(ParquetPojo.class) + "/.metadata").exists());
	}


}
