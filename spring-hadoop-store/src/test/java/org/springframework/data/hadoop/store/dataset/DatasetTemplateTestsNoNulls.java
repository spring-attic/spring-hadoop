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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.test.tests.Assume;
import org.springframework.data.hadoop.test.tests.Version;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"DatasetTemplateTests-context.xml"})
public class DatasetTemplateTestsNoNulls extends AbstractDatasetTemplateTests {

	@Autowired
	public void setDatasetOperations(DatasetOperations datasetOperations) {
		((DatasetTemplate)datasetOperations).setDefaultDatasetDefinition(new DatasetDefinition(false));
		this.datasetOperations = datasetOperations;
	}

	@Test(expected = org.apache.avro.file.DataFileWriter.AppendWriteException.class)
	public void testWritePojoWithNullValuesShouldFail() {
		//Kite SDK currently uses some Hadoop 2.0 only methods
		Assume.hadoopVersion(Version.HADOOP2X);
		datasetOperations.write(records);
		TestPojo pojo4 = new TestPojo();
		pojo4.setId(33L);
		pojo4.setName(null);
		pojo4.setBirthDate(null);
		datasetOperations.write(Collections.singletonList(pojo4));
	}

}
