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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.util.Assert;

public abstract class AbstractRemoteJobExecutionDaoTests {

	protected JobExecutionDao dao;
	protected JobExecution execution;
	protected StepExecutionDao stepExecutionDao;
	protected JobParameters jobParameters;
	protected JobInstance jobInstance;

	@Before
	public void onSetUp() throws Exception {
		dao = getRemoteJobExecutionDao();
		jobParameters = new JobParameters();
		stepExecutionDao = getRemoteStepExecutionDao();
		JobInstanceDao jobInstanceDao = getRemoteJobInstanceDao();
		jobInstance = jobInstanceDao.createJobInstance("execTestJob", jobParameters);
		execution = new JobExecution(jobInstance);
	}

	protected abstract RemoteJobInstanceDao getRemoteJobInstanceDao();
	protected abstract RemoteJobExecutionDao getRemoteJobExecutionDao();
	protected abstract RemoteStepExecutionDao getRemoteStepExecutionDao();

	@Test
	public void testSaveAndFind() {

		execution.setStartTime(new Date(System.currentTimeMillis()));
		execution.setLastUpdated(new Date(System.currentTimeMillis()));
		execution.setExitStatus(ExitStatus.UNKNOWN);
		execution.setEndTime(new Date(System.currentTimeMillis()));
		dao.saveJobExecution(execution);

		List<JobExecution> executions = dao.findJobExecutions(jobInstance);
		assertEquals(1, executions.size());
		assertEquals(execution, executions.get(0));
		assertExecutionsAreEqual(execution, executions.get(0));
	}

	@Test
	public void testFindExecutionsOrdering() {

		List<JobExecution> execs = new ArrayList<JobExecution>();

		for (int i = 0; i < 10; i++) {
//            JobExecution exec = new JobExecution(jobInstance, jobParameters);
			JobExecution exec = new JobExecution(jobInstance);
			exec.setCreateTime(new Date(i));
			execs.add(exec);
			dao.saveJobExecution(exec);
		}

		List<JobExecution> retrieved = dao.findJobExecutions(jobInstance);
		Collections.reverse(retrieved);

		for (int i = 0; i < 10; i++) {
			assertExecutionsAreEqual(execs.get(i), retrieved.get(i));
		}

	}

	/**
	 * Save and find a job execution.
	 */
	@Test
	public void testFindNonExistentExecutions() {
		List<JobExecution> executions = dao.findJobExecutions(jobInstance);
		assertEquals(0, executions.size());
	}

	/**
	 * Saving sets id to the entity.
	 */
	@Test
	public void testSaveAddsIdAndVersion() {
		assertNull(execution.getId());
		assertNull(execution.getVersion());
		dao.saveJobExecution(execution);
		assertNotNull(execution.getId());
		assertNotNull(execution.getVersion());
	}

	/**
	 * Update and retrieve job execution - check attributes have changed as
	 * expected.
	 */
	@Test
	public void testUpdateExecution() {
		execution.setStatus(BatchStatus.STARTED);
		dao.saveJobExecution(execution);

		execution.setLastUpdated(new Date(0));
		execution.setStatus(BatchStatus.COMPLETED);
		dao.updateJobExecution(execution);

		JobExecution updated = dao.findJobExecutions(jobInstance).get(0);
		assertEquals(execution, updated);
		assertEquals(BatchStatus.COMPLETED, updated.getStatus());
		assertExecutionsAreEqual(execution, updated);
	}

	/**
	 * Check the execution with most recent start time is returned
	 */
	@Test
	public void testGetLastExecution() {
		JobExecution exec1 = new JobExecution(jobInstance);
		exec1.setCreateTime(new Date(0));

		JobExecution exec2 = new JobExecution(jobInstance);
		exec2.setCreateTime(new Date(1));

		dao.saveJobExecution(exec1);
		dao.saveJobExecution(exec2);

		JobExecution last = dao.getLastJobExecution(jobInstance);
		assertEquals(exec2, last);
	}

	/**
	 * Check the execution is returned
	 */
	@Test
	public void testGetMissingLastExecution() {
		JobExecution value = dao.getLastJobExecution(jobInstance);
		assertNull(value);
	}

	/**
	 * Check the execution is returned
	 */
	@Test
	public void testFindRunningExecutions() {
		JobExecution exec = new JobExecution(jobInstance);
		exec.setCreateTime(new Date(0));
		exec.setEndTime(new Date(1L));
		exec.setLastUpdated(new Date(5L));
		dao.saveJobExecution(exec);

		exec = new JobExecution(jobInstance);
		exec.setLastUpdated(new Date(5L));
		exec.createStepExecution("step");
		dao.saveJobExecution(exec);

		if (stepExecutionDao != null) {
			for (StepExecution stepExecution : exec.getStepExecutions()) {
				stepExecutionDao.saveStepExecution(stepExecution);
			}
		}

		Set<JobExecution> values = dao.findRunningJobExecutions(exec.getJobInstance().getJobName());

		assertEquals(1, values.size());
		JobExecution value = values.iterator().next();
		assertEquals(exec, value);
		assertEquals(5L, value.getLastUpdated().getTime());

	}

	/**
	 * Check the execution is returned
	 */
	@Test
	public void testNoRunningExecutions() {
		Set<JobExecution> values = dao.findRunningJobExecutions("no-such-job");
		assertEquals(0, values.size());
	}

	/**
	 * Check the execution is returned
	 */
	@Test
	public void testGetExecution() {
		JobExecution exec = new JobExecution(jobInstance);
		exec.setCreateTime(new Date(0));
		exec.createStepExecution("step");

		dao.saveJobExecution(exec);
		if (stepExecutionDao != null) {
			for (StepExecution stepExecution : exec.getStepExecutions()) {
				stepExecutionDao.saveStepExecution(stepExecution);
			}
		}
		JobExecution value = dao.getJobExecution(exec.getId());

		assertEquals(exec, value);
		// N.B. the job instance is not re-hydrated in the JDBC case...
	}

	/**
	 * Check the execution is returned
	 */
	@Test
	public void testGetMissingExecution() {
		JobExecution value = dao.getJobExecution(54321L);
		assertNull(value);
	}

	// TODO: enable this test when we get errors via remote calls
//    /**
//     * Exception should be raised when the version of update argument doesn't
//     * match the version of persisted entity.
//     */
//    @Test
//    public void testConcurrentModificationException() {
//
//        JobExecution exec1 = new JobExecution(jobInstance);
//        dao.saveJobExecution(exec1);
//
//        JobExecution exec2 = new JobExecution(jobInstance);
//        exec2.setId(exec1.getId());
//
//        exec2.incrementVersion();
//        assertEquals((Integer) 0, exec1.getVersion());
//        assertEquals(exec1.getVersion(), exec2.getVersion());
//
//        dao.updateJobExecution(exec1);
//        assertEquals((Integer) 1, exec1.getVersion());
//
//        try {
//            dao.updateJobExecution(exec2);
//            fail();
//        }
//        catch (OptimisticLockingFailureException e) {
//            // expected
//        }
//
//    }

	/**
	 * Successful synchronization from STARTED to STOPPING status.
	 */
	@Test
	public void testSynchronizeStatusUpgrade() {

		JobExecution exec1 = new JobExecution(jobInstance);
		exec1.setStatus(BatchStatus.STOPPING);
		dao.saveJobExecution(exec1);

		JobExecution exec2 = new JobExecution(jobInstance);
		Assert.state(exec1.getId() != null);
		exec2.setId(exec1.getId());

		exec2.setStatus(BatchStatus.STARTED);
		exec2.setVersion(7);
		Assert.state(exec1.getVersion() != exec2.getVersion());
		Assert.state(exec1.getStatus() != exec2.getStatus());

		dao.synchronizeStatus(exec2);

		assertEquals(exec1.getVersion(), exec2.getVersion());
		assertEquals(exec1.getStatus(), exec2.getStatus());
	}

	/**
	 * UNKNOWN status won't be changed by synchronizeStatus, because it is the
	 * 'largest' BatchStatus (will not downgrade).
	 */
	@Test
	public void testSynchronizeStatusDowngrade() {

		JobExecution exec1 = new JobExecution(jobInstance);
		exec1.setStatus(BatchStatus.STARTED);
		dao.saveJobExecution(exec1);

		JobExecution exec2 = new JobExecution(jobInstance);
		Assert.state(exec1.getId() != null);
		exec2.setId(exec1.getId());

		exec2.setStatus(BatchStatus.UNKNOWN);
		exec2.setVersion(7);
		Assert.state(exec1.getVersion() != exec2.getVersion());
		Assert.state(exec1.getStatus().isLessThan(exec2.getStatus()));

		dao.synchronizeStatus(exec2);

		assertEquals(exec1.getVersion(), exec2.getVersion());
		assertEquals(BatchStatus.UNKNOWN, exec2.getStatus());
	}


	/*
	 * Check to make sure the executions are equal. Normally, comparing the id's
	 * is sufficient. However, for testing purposes, especially of a DAO, we
	 * need to make sure all the fields are being stored/retrieved correctly.
	 */
	private void assertExecutionsAreEqual(JobExecution lhs, JobExecution rhs) {
		assertEquals(lhs.getId(), rhs.getId());
		assertEquals(lhs.getStartTime(), rhs.getStartTime());
		assertEquals(lhs.getStatus(), rhs.getStatus());
		assertEquals(lhs.getEndTime(), rhs.getEndTime());
		assertEquals(lhs.getCreateTime(), rhs.getCreateTime());
		assertEquals(lhs.getLastUpdated(), rhs.getLastUpdated());
		assertEquals(lhs.getVersion(), rhs.getVersion());
	}


}
