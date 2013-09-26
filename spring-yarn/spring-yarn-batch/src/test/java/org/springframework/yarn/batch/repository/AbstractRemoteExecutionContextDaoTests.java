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
package org.springframework.yarn.batch.repository;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.item.ExecutionContext;

public abstract class AbstractRemoteExecutionContextDaoTests {

	private JobInstanceDao jobInstanceDao;
	private JobExecutionDao jobExecutionDao;
	private StepExecutionDao stepExecutionDao;
	private ExecutionContextDao contextDao;
	private JobExecution jobExecution;
	private StepExecution stepExecution;

	@Before
	public void setUp() {
		jobInstanceDao = getRemoteJobInstanceDao();
		jobExecutionDao = getRemoteJobExecutionDao();
		stepExecutionDao = getRemoteStepExecutionDao();
		contextDao = getRemoteExecutionContextDao();

		JobInstance ji = jobInstanceDao.createJobInstance("testJob", new JobParameters());
		jobExecution = new JobExecution(ji, new JobParameters());

		jobExecutionDao.saveJobExecution(jobExecution);
		stepExecution = new StepExecution("stepName", jobExecution);
		stepExecutionDao.saveStepExecution(stepExecution);
	}

	protected abstract RemoteJobInstanceDao getRemoteJobInstanceDao();
	protected abstract RemoteJobExecutionDao getRemoteJobExecutionDao();
	protected abstract RemoteStepExecutionDao getRemoteStepExecutionDao();
	protected abstract RemoteExecutionContextDao getRemoteExecutionContextDao();

	@Test
	public void testSaveAndFindJobContext() {
		ExecutionContext ctx = new ExecutionContext(Collections.<String, Object> singletonMap("key", "value"));
		jobExecution.setExecutionContext(ctx);
		contextDao.saveExecutionContext(jobExecution);

		ExecutionContext retrieved = contextDao.getExecutionContext(jobExecution);
		assertEquals(ctx, retrieved);
	}

	@Test
	public void testSaveAndFindExecutionContexts() {

		List<StepExecution> stepExecutions = new ArrayList<StepExecution>();
		for (int i = 0; i < 3; i++) {
			JobInstance ji = jobInstanceDao.createJobInstance("testJob" + i, new JobParameters());
			JobExecution je = new JobExecution(ji, new JobParameters());
			jobExecutionDao.saveJobExecution(je);
			StepExecution se = new StepExecution("step" + i, je);
			se.setStatus(BatchStatus.STARTED);
			se.setReadSkipCount(i);
			se.setProcessSkipCount(i);
			se.setWriteSkipCount(i);
			se.setProcessSkipCount(i);
			se.setRollbackCount(i);
			se.setLastUpdated(new Date(System.currentTimeMillis()));
			se.setReadCount(i);
			se.setFilterCount(i);
			se.setWriteCount(i);
			stepExecutions.add(se);
		}
		stepExecutionDao.saveStepExecutions(stepExecutions);
		contextDao.saveExecutionContexts(stepExecutions);

		for (int i = 0; i < 3; i++) {
			ExecutionContext retrieved = contextDao.getExecutionContext(stepExecutions.get(i).getJobExecution());
			assertEquals(stepExecutions.get(i).getExecutionContext(), retrieved);
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSaveNullExecutionContexts() {
		contextDao.saveExecutionContexts(null);
	}

	@Test
	public void testSaveEmptyExecutionContexts() {
		contextDao.saveExecutionContexts(new ArrayList<StepExecution>());
	}

	@Test
	public void testSaveAndFindEmptyJobContext() {

		ExecutionContext ctx = new ExecutionContext();
		jobExecution.setExecutionContext(ctx);
		contextDao.saveExecutionContext(jobExecution);

		ExecutionContext retrieved = contextDao.getExecutionContext(jobExecution);
		assertEquals(ctx, retrieved);
	}

	@Test
	public void testUpdateContext() {

		ExecutionContext ctx = new ExecutionContext(Collections
				.<String, Object> singletonMap("key", "value"));
		jobExecution.setExecutionContext(ctx);
		contextDao.saveExecutionContext(jobExecution);

		ctx.putLong("longKey", 7);
		contextDao.updateExecutionContext(jobExecution);

		ExecutionContext retrieved = contextDao.getExecutionContext(jobExecution);
		assertEquals(ctx, retrieved);
		assertEquals(7, retrieved.getLong("longKey"));
	}

	@Test
	public void testSaveAndFindStepContext() {

		ExecutionContext ctx = new ExecutionContext(Collections.<String, Object> singletonMap("key", "value"));
		stepExecution.setExecutionContext(ctx);
		contextDao.saveExecutionContext(stepExecution);

		ExecutionContext retrieved = contextDao.getExecutionContext(stepExecution);
		assertEquals(ctx, retrieved);
	}

	@Test
	public void testSaveAndFindEmptyStepContext() {

		ExecutionContext ctx = new ExecutionContext();
		stepExecution.setExecutionContext(ctx);
		contextDao.saveExecutionContext(stepExecution);

		ExecutionContext retrieved = contextDao.getExecutionContext(stepExecution);
		assertEquals(ctx, retrieved);
	}

	@Test
	public void testUpdateStepContext() {

		ExecutionContext ctx = new ExecutionContext(Collections.<String, Object> singletonMap("key", "value"));
		stepExecution.setExecutionContext(ctx);
		contextDao.saveExecutionContext(stepExecution);

		ctx.putLong("longKey", 7);
		contextDao.updateExecutionContext(stepExecution);

		ExecutionContext retrieved = contextDao.getExecutionContext(stepExecution);
		assertEquals(ctx, retrieved);
		assertEquals(7, retrieved.getLong("longKey"));
	}

	@Test
	public void testStoreInteger() {
		ExecutionContext ec = new ExecutionContext();
		ec.put("intValue", new Integer(343232));
		stepExecution.setExecutionContext(ec);
		contextDao.saveExecutionContext(stepExecution);
		ExecutionContext restoredEc = contextDao.getExecutionContext(stepExecution);
		assertEquals(ec, restoredEc);
	}

}
