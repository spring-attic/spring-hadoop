/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.batch.item;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HdfsItemReaderTest {

	@Autowired
	ApplicationContext ctx;
	@Autowired
	ResourceAwareItemReaderItemStream<?> reader;
	@Autowired
	MultiResourceItemReader<?> multiReader;

	@Test
	public void testSingleReader() throws Exception {
		try {
			reader.open(new ExecutionContext());
			assertNotNull(reader.read());
		} finally {
			reader.close();
		}
	}

	@Test
	public void testMultiReader() throws Exception {
		assertNotNull(multiReader);
		try {
			multiReader.open(new ExecutionContext());
			assertNotNull(multiReader.read());
		} finally {
			reader.close();
		}
	}
}
