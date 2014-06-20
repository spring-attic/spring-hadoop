/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.store.input.TextFileReader;
import org.springframework.data.hadoop.store.output.TextFileWriter;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for reading and writing text using context configuration.
 *
 * @author liu jiong
 *
 */
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class TextFileStoreAppendCtxTests extends AbstractStoreTests {

	@Autowired
	private ApplicationContext context;

	@Test
	public void testWriteReadManyLines() throws IOException, InterruptedException {

		TextFileWriter writer = context.getBean("writer", TextFileWriter.class);
		assertNotNull(writer);
        String[] dataArray = new String[] { DATA10,DATA11 };
        writer.setAppendable(true);
        TextFileReader reader = new TextFileReader(getConfiguration(), writer.getPath(), null);
        String[] strings =null;
        if(writer.getPath().getFileSystem(writer.getConfiguration()).exists(writer.getPath())) {
            List<String> tmpList = TestUtils.readData(reader);
            strings = new String[tmpList.size()];
            tmpList.toArray(strings);
        }
        for(String data:dataArray) {
            writer.write(data);
        }
        writer.resetIdleTimeout();
        Thread.sleep(5000);
        reader = new TextFileReader(getConfiguration(), writer.getPath(), null);
        TestUtils.readDataAndAssert(reader, (String[])ArrayUtils.addAll(strings,dataArray));
	}

}
