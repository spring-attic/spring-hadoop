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

import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;

public class AvroPojoDatasetStoreWriterTests extends AbstractDatasetStoreWriterTests<TestPojo> {

	@Autowired
	public void setDatasetRepositoryFactory(DatasetRepositoryFactory datasetRepositoryFactory) {
		this.datasetStoreWriter = new AvroPojoDatasetStoreWriter<TestPojo>(TestPojo.class, datasetRepositoryFactory);
		this.datasetOperations = new DatasetTemplate(datasetRepositoryFactory);
		this.recordClass = TestPojo.class;
	}

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
	}

}
