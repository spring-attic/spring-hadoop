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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.support.SimpleJobRepository;

/**
 * Tests for {@link org.springframework.batch.core.repository.dao.StepExecutionDao} using
 * {@link RemoteStepExecutionDao}.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractRemoteStepExecutionDaoTests {

	protected StepExecutionDao dao;
	protected JobInstance jobInstance;
	protected JobExecution jobExecution;
	protected Step step;
	protected StepExecution stepExecution;
	protected JobRepository repository;

	@Before
	public void onSetUp() throws Exception {
		dao = getRemoteStepExecutionDao();
		repository = new SimpleJobRepository(getRemoteJobInstanceDao(), getRemoteJobExecutionDao(),
				dao, getRemoteExecutionContextDao());
		jobExecution = repository.createJobExecution("job", new JobParameters());
		jobInstance = jobExecution.getJobInstance();
		step = new StepSupport("foo");
		stepExecution = new StepExecution(step.getName(), jobExecution);
	}

	protected abstract RemoteJobInstanceDao getRemoteJobInstanceDao();
	protected abstract RemoteJobExecutionDao getRemoteJobExecutionDao();
	protected abstract RemoteStepExecutionDao getRemoteStepExecutionDao();
	protected abstract RemoteExecutionContextDao getRemoteExecutionContextDao();

	@Test
	public void testSaveExecutionAssignsIdAndVersion() throws Exception {
		assertNull(stepExecution.getId());
		assertNull(stepExecution.getVersion());
		dao.saveStepExecution(stepExecution);
		assertNotNull(stepExecution.getId());
		assertNotNull(stepExecution.getVersion());
	}

	@Test
	public void testSaveAndGetExecution() {

		stepExecution.setStatus(BatchStatus.STARTED);
		stepExecution.setReadSkipCount(7);
		stepExecution.setProcessSkipCount(2);
		stepExecution.setWriteSkipCount(5);
		stepExecution.setProcessSkipCount(11);
		stepExecution.setRollbackCount(3);
		stepExecution.setLastUpdated(new Date(System.currentTimeMillis()));
		stepExecution.setReadCount(17);
		stepExecution.setFilterCount(15);
		stepExecution.setWriteCount(13);
		dao.saveStepExecution(stepExecution);

		StepExecution retrieved = dao.getStepExecution(jobExecution, stepExecution.getId());

		assertStepExecutionsAreEqual(stepExecution, retrieved);
		assertNotNull(retrieved.getVersion());
		assertNotNull(retrieved.getJobExecution());
		assertNotNull(retrieved.getJobExecution().getId());
		assertNotNull(retrieved.getJobExecution().getJobId());
		assertNotNull(retrieved.getJobExecution().getJobInstance());

	}

	@Test
	public void testSaveAndGetNonExistentExecution() {
		assertNull(dao.getStepExecution(jobExecution, 45677L));
	}

	@Test
	public void testSaveAndFindExecution() {

		stepExecution.setStatus(BatchStatus.STARTED);
		stepExecution.setReadSkipCount(7);
		stepExecution.setWriteSkipCount(5);
		stepExecution.setRollbackCount(3);
		dao.saveStepExecution(stepExecution);

		dao.addStepExecutions(jobExecution);
		Collection<StepExecution> retrieved = jobExecution.getStepExecutions();

		assertStepExecutionsAreEqual(stepExecution, retrieved.iterator().next());
	}

	@Test
	public void testGetForNotExistingJobExecution() {
		assertNull(dao.getStepExecution(new JobExecution(jobInstance, (long) 777), 11L));
	}

	/**
	 * To-be-saved execution must not already have an id.
	 */
	@Test
	public void testSaveExecutionWithIdAlreadySet() {
		stepExecution.setId((long) 7);
		try {
			dao.saveStepExecution(stepExecution);
			fail();
		}
		catch (IllegalArgumentException e) {
			// expected
		}
	}

	/**
	 * To-be-saved execution must not already have a version.
	 */
	@Test
	public void testSaveExecutionWithVersionAlreadySet() {
		stepExecution.incrementVersion();
		try {
			dao.saveStepExecution(stepExecution);
			fail();
		}
		catch (IllegalArgumentException e) {
			// expected
		}
	}

	/**
	 * Update and retrieve updated StepExecution - make sure the update is
	 * reflected as expected and version number has been incremented
	 */
	@Test
	public void testUpdateExecution() {
		stepExecution.setStatus(BatchStatus.STARTED);
		dao.saveStepExecution(stepExecution);
		Integer versionAfterSave = stepExecution.getVersion();

		stepExecution.setStatus(BatchStatus.ABANDONED);
		stepExecution.setLastUpdated(new Date(System.currentTimeMillis()));
		dao.updateStepExecution(stepExecution);
		assertEquals(versionAfterSave + 1, stepExecution.getVersion().intValue());

		StepExecution retrieved = dao.getStepExecution(jobExecution, stepExecution.getId());
		assertEquals(stepExecution, retrieved);
		assertEquals(stepExecution.getLastUpdated(), retrieved.getLastUpdated());
		assertEquals(BatchStatus.ABANDONED, retrieved.getStatus());
	}

	/**
	 * Exception should be raised when the version of update argument doesn't
	 * match the version of persisted entity.
	 */
//    @Test
//    public void testConcurrentModificationException() {
//        step = new StepSupport("foo");
//
//        StepExecution exec1 = new StepExecution(step.getName(), jobExecution);
//        dao.saveStepExecution(exec1);
//
//        StepExecution exec2 = new StepExecution(step.getName(), jobExecution);
//        exec2.setId(exec1.getId());
//
//        exec2.incrementVersion();
//        assertEquals(new Integer(0), exec1.getVersion());
//        assertEquals(exec1.getVersion(), exec2.getVersion());
//
//        dao.updateStepExecution(exec1);
//        assertEquals(new Integer(1), exec1.getVersion());
//
//        try {
//            dao.updateStepExecution(exec2);
//            fail();
//        }
//        catch (OptimisticLockingFailureException e) {
//            // expected
//        }
//
//    }

	@Test
	public void testGetStepExecutionsWhenNoneExist() throws Exception {
		int count = jobExecution.getStepExecutions().size();
		dao.addStepExecutions(jobExecution);
		assertEquals("Incorrect size of collection", count, jobExecution.getStepExecutions().size());
	}



	private void assertStepExecutionsAreEqual(StepExecution expected, StepExecution actual) {
		assertEquals(expected.getId(), actual.getId());
		assertEquals(expected.getStartTime(), actual.getStartTime());
		assertEquals(expected.getEndTime(), actual.getEndTime());
		assertEquals(expected.getSkipCount(), actual.getSkipCount());
		assertEquals(expected.getCommitCount(), actual.getCommitCount());
		assertEquals(expected.getReadCount(), actual.getReadCount());
		assertEquals(expected.getWriteCount(), actual.getWriteCount());
		assertEquals(expected.getFilterCount(), actual.getFilterCount());
		assertEquals(expected.getWriteSkipCount(), actual.getWriteSkipCount());
		assertEquals(expected.getReadSkipCount(), actual.getReadSkipCount());
		assertEquals(expected.getProcessSkipCount(), actual.getProcessSkipCount());
		assertEquals(expected.getRollbackCount(), actual.getRollbackCount());
		assertEquals(expected.getExitStatus(), actual.getExitStatus());
		assertEquals(expected.getLastUpdated(), actual.getLastUpdated());
		assertEquals(expected.getExitStatus(), actual.getExitStatus());
		assertEquals(expected.getJobExecutionId(), actual.getJobExecutionId());
	}

}
