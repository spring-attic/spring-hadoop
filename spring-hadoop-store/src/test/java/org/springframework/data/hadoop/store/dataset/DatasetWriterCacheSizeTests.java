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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.DatasetRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractHadoopClusterTests;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class, locations={"DatasetTemplateTests-context.xml"})
@MiniHadoopCluster
public class DatasetWriterCacheSizeTests  extends AbstractHadoopClusterTests {

	private DatasetRepositoryFactory datasetRepositoryFactory;
	private List<SimplePojo> records = new ArrayList<SimplePojo>();
	@Autowired
	private String path;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");

	@Autowired
	public void setDatasetRepositoryFactory(DatasetRepositoryFactory datasetRepositoryFactory) {
		this.datasetRepositoryFactory = datasetRepositoryFactory;
	}

	@Before
	public void setUp() throws ParseException {
		records.add(new SimplePojo(1L, "Sven", dateFormat.parse("1997/11/01").getTime()));
		records.add(new SimplePojo(2L, "Nisse", dateFormat.parse("1997/11/02").getTime()));
		records.add(new SimplePojo(3L, "Mary", dateFormat.parse("1997/12/03").getTime()));
		records.add(new SimplePojo(4L, "Anna", dateFormat.parse("1997/08/04").getTime()));
		records.add(new SimplePojo(5L, "Pelle", dateFormat.parse("1997/05/05").getTime()));
		records.add(new SimplePojo(6L, "Maria", dateFormat.parse("1997/04/06").getTime()));
		records.add(new SimplePojo(7L, "Molly", dateFormat.parse("1997/02/07").getTime()));
		records.add(new SimplePojo(8L, "Peter", dateFormat.parse("1997/03/08").getTime()));
		records.add(new SimplePojo(9L, "Frank", dateFormat.parse("1997/01/09").getTime()));
		records.add(new SimplePojo(10L, "Albin", dateFormat.parse("1997/07/10").getTime()));
		records.add(new SimplePojo(11L, "Jonas", dateFormat.parse("1997/06/11").getTime()));
		records.add(new SimplePojo(12L, "Sandy", dateFormat.parse("1997/09/12").getTime()));
		records.add(new SimplePojo(13L, "Arne", dateFormat.parse("1997/10/13").getTime()));
		records.add(new SimplePojo(14L, "Paula", dateFormat.parse("1997/08/14").getTime()));
		records.add(new SimplePojo(15L, "Elisabeth", dateFormat.parse("1997/05/15").getTime()));
		records.add(new SimplePojo(16L, "William", dateFormat.parse("1997/10/16").getTime()));
		records.add(new SimplePojo(17L, "Bernice", dateFormat.parse("1997/11/17").getTime()));
		records.add(new SimplePojo(18L, "Irma", dateFormat.parse("1997/12/18").getTime()));
		records.add(new SimplePojo(19L, "Siri", dateFormat.parse("1997/11/19").getTime()));

	}

	@Test
	public void runWithoutSettingWriterCacheSize() throws IOException {
		PartitionStrategy partitionStrategy =
				new PartitionStrategy.Builder().year("birthDate").month("birthDate").build();
		DatasetDefinition datasetDefinition = new DatasetDefinition(SimplePojo.class, "avro", partitionStrategy);
		final DatasetTemplate datasetTemplete = new DatasetTemplate(datasetRepositoryFactory, datasetDefinition);

		// clean up
		datasetTemplete.execute(new DatasetRepositoryCallback() {
			@Override
			public void doInRepository(DatasetRepository datasetRepository) {
				datasetRepository.delete("test", datasetTemplete.getDatasetName(SimplePojo.class));
			}
		});

		AvroPojoDatasetStoreWriter<SimplePojo> writer =
				new AvroPojoDatasetStoreWriter<SimplePojo>(SimplePojo.class, datasetRepositoryFactory, datasetDefinition);
		for (SimplePojo pojo: records) {
			writer.write(pojo);
		};
		writer.flush();
		writer.close();

		// closing shell would close minicluster fs and break tests
		@SuppressWarnings("resource")
		FsShell fsShell = new FsShell(getConfiguration());
		assertTrue(fsShell.ls(true, path + "/test/" + datasetTemplete.getDatasetName(SimplePojo.class) + "/year=1997/month=05/*.avro").size() >= 2);
		assertTrue(fsShell.ls(true, path + "/test/" + datasetTemplete.getDatasetName(SimplePojo.class) + "/year=1997/month=11/*.avro").size() >= 2);
		assertTrue(fsShell.ls(true, path + "/test/" + datasetTemplete.getDatasetName(SimplePojo.class) + "/year=1997/month=12/*.avro").size() >= 2);
	}

	@Test
	public void runSettingWriterCacheSize() throws IOException {
		PartitionStrategy partitionStrategy =
				new PartitionStrategy.Builder().year("birthDate").month("birthDate").build();
		DatasetDefinition datasetDefinition = new DatasetDefinition(SimplePojo.class, "avro", partitionStrategy);
		datasetDefinition.setWriterCacheSize(20);
		final DatasetTemplate datasetTemplete = new DatasetTemplate(datasetRepositoryFactory, datasetDefinition);

		// clean up
		datasetTemplete.execute(new DatasetRepositoryCallback() {
			@Override
			public void doInRepository(DatasetRepository datasetRepository) {
				datasetRepository.delete("test", datasetTemplete.getDatasetName(SimplePojo.class));
			}
		});

		AvroPojoDatasetStoreWriter<SimplePojo> writer =
				new AvroPojoDatasetStoreWriter<SimplePojo>(SimplePojo.class, datasetRepositoryFactory, datasetDefinition);
		for (SimplePojo pojo: records) {
			writer.write(pojo);
		};
		writer.flush();
		writer.close();

		// closing shell would close minicluster fs and break tests
		@SuppressWarnings("resource")
		FsShell fsShell = new FsShell(getConfiguration());
		assertEquals(1, fsShell.ls(true, path + "/test/" + datasetTemplete.getDatasetName(SimplePojo.class) + "/year=1997/month=05/*.avro").size());
		assertEquals(1, fsShell.ls(true, path + "/test/" + datasetTemplete.getDatasetName(SimplePojo.class) + "/year=1997/month=11/*.avro").size());
		assertEquals(1, fsShell.ls(true, path + "/test/" + datasetTemplete.getDatasetName(SimplePojo.class) + "/year=1997/month=12/*.avro").size());
	}
}
