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
package org.springframework.data.hadoop.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.serializer.DefaultSerializer;
import org.springframework.core.serializer.Serializer;
import org.springframework.data.hadoop.batch.item.HdfsItemWriter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * @author Michael Minella
 *
 */
public class HdfsItemWriterTest {

	private HdfsItemWriter<String> writer;
	@SuppressWarnings("rawtypes")
	private Serializer itemSerializer;
	private final String fileName = "/tmp/myFile.txt";

	@Mock
	private FileSystem fileSystem;
	@Mock
	private FSDataOutputStream fsDataOutputStream;
	private PlatformTransactionManager transactionManager = new ResourcelessTransactionManager();

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		itemSerializer = new DefaultSerializer();
		writer = new HdfsItemWriter<String>(fileSystem, itemSerializer, fileName);
	}

	@Test
	@SuppressWarnings("serial")
	public void testWriteNoTransaction() throws Exception {
		List<String> items = new ArrayList<String>() {{
			add(new String("one"));
			add(new String("two"));
		}};

		when(fileSystem.createNewFile(new Path(fileName))).thenReturn(true);
		when(fileSystem.create(new Path(fileName))).thenReturn(fsDataOutputStream);

		writer.open(null);
		writer.write(items);

		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		getBytes(items.get(0), stream);
		getBytes(items.get(1), stream);

		verify(fsDataOutputStream).write(stream.toByteArray());
	}

	@Test
	public void testWriteNoTransactionNoItems() throws Exception {
		when(fileSystem.createNewFile(new Path(fileName))).thenReturn(true);
		when(fileSystem.create(new Path(fileName))).thenReturn(fsDataOutputStream);

		writer.open(null);
		writer.write(new ArrayList<String>());

		verifyZeroInteractions(fsDataOutputStream);
	}

	@Test
	@SuppressWarnings("serial")
	public void testWriteTransaction() throws Exception {
		final List<String> items = new ArrayList<String>() {{
			add(new String("one"));
			add(new String("two"));
		}};

		when(fileSystem.createNewFile(new Path(fileName))).thenReturn(true);
		when(fileSystem.create(new Path(fileName))).thenReturn(fsDataOutputStream);

		writer.open(null);

		new TransactionTemplate(transactionManager).execute(new TransactionCallback<Object>() {
			@Override
			public Object doInTransaction(TransactionStatus status) {
				try {
					writer.write(items);
				} catch (Exception e) {
					fail("An exception was thrown while writing: " + e.getMessage());
				}

				return null;
			}
		});

		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		getBytes(items.get(0), stream);
		getBytes(items.get(1), stream);

		verify(fsDataOutputStream).write(stream.toByteArray());
	}

	@Test
	@SuppressWarnings("serial")
	public void testWriteTransactionFails() throws Exception {
		final List<String> items = new ArrayList<String>() {{
			add(new String("one"));
			add(new String("two"));
		}};

		when(fileSystem.createNewFile(new Path(fileName))).thenReturn(true);
		when(fileSystem.create(new Path(fileName))).thenReturn(fsDataOutputStream);

		writer.open(null);

		try {
			new TransactionTemplate(transactionManager).execute(new TransactionCallback<Object>() {
				@Override
				public Object doInTransaction(TransactionStatus status) {
					try {
						writer.write(items);
					} catch (Exception e) {
						fail("An exception was thrown while writing: " + e.getMessage());
					}

					throw new RuntimeException("force rollback");
				}
			});
		} catch (RuntimeException re) {
			assertEquals(re.getMessage(), "force rollback");
		} catch (Throwable t) {
			fail("Unexpected exception was thrown: " + t.getMessage());
		}

		verifyZeroInteractions(fsDataOutputStream);
	}

	/**
	 * A pointless use case but validates that the flag is still honored.
	 *
	 * @throws Exception
	 */
	@Test
	@SuppressWarnings("serial")
	public void testWriteTransactionReadOnly() throws Exception {
		final List<String> items = new ArrayList<String>() {{
			add(new String("one"));
			add(new String("two"));
		}};

		when(fileSystem.createNewFile(new Path(fileName))).thenReturn(true);
		when(fileSystem.create(new Path(fileName))).thenReturn(fsDataOutputStream);

		writer.open(null);

		try {
			TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
			transactionTemplate.setReadOnly(true);
			transactionTemplate.execute(new TransactionCallback<Object>() {

				@Override
				public Object doInTransaction(TransactionStatus status) {
					try {
						writer.write(items);
					} catch (Exception e) {
						fail("An exception was thrown while writing: " + e.getMessage());
					}

					return null;
				}
			});
		} catch (Throwable t) {
			fail("Unexpected exception was thrown: " + t.getMessage());
		}

		verifyZeroInteractions(fsDataOutputStream);
	}

	@Test
	public void testWithinJob() throws Exception {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("/org/springframework/data/hadoop/fs/HdfsItemWriterTest-context.xml");
		JobLauncher launcher = context.getBean(JobLauncher.class);
		Job job = context.getBean(Job.class);

		JobParameters jobParameters = new JobParametersBuilder().toJobParameters();

		JobExecution execution = launcher.run(job, jobParameters);
		assertTrue("status was: " + execution.getStatus(), execution.getStatus() == BatchStatus.COMPLETED);
		context.close();
	}

	@SuppressWarnings("unchecked")
	private void getBytes(Object src, ByteArrayOutputStream stream) {
		try {
			itemSerializer.serialize(src, stream);
		} catch (IOException ignore) {
		}
	}
}
