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
package org.springframework.yarn.batch.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;

public abstract class AbstractJobRepositoryTests {

    private JobRepository jobRepository;

    protected JobSupport job = new JobSupport("AbstractJobRepositoryTests");

    protected JobParameters jobParameters = new JobParameters();

	@Before
	public void setUp() {
		jobRepository = getJobRepository();
	}

	protected abstract JobRepository getJobRepository();

    @Test
    public void testCreateAndFind() throws Exception {

        job.setRestartable(true);

        JobParametersBuilder builder = new JobParametersBuilder();
        builder.addString("stringKey", "stringValue").addLong("longKey", 1L).addDouble("doubleKey", 1.1).addDate(
                "dateKey", new Date(1L));
        JobParameters jobParams = builder.toJobParameters();

        JobExecution firstExecution = jobRepository.createJobExecution(job.getName(), jobParams);
        firstExecution.setStartTime(new Date());
        assertNotNull(firstExecution.getLastUpdated());

        assertEquals(job.getName(), firstExecution.getJobInstance().getJobName());

        jobRepository.update(firstExecution);
        firstExecution.setEndTime(new Date());
        jobRepository.update(firstExecution);
        JobExecution secondExecution = jobRepository.createJobExecution(job.getName(), jobParams);

        assertEquals(firstExecution.getJobInstance(), secondExecution.getJobInstance());
        assertEquals(job.getName(), secondExecution.getJobInstance().getJobName());
    }

    @Test
    public void testCreateAndFindWithNoStartDate() throws Exception {
        job.setRestartable(true);

        JobExecution firstExecution = jobRepository.createJobExecution(job.getName(), jobParameters);
        firstExecution.setStartTime(new Date(0));
        firstExecution.setEndTime(new Date(1));
        jobRepository.update(firstExecution);
        JobExecution secondExecution = jobRepository.createJobExecution(job.getName(), jobParameters);

        assertEquals(firstExecution.getJobInstance(), secondExecution.getJobInstance());
        assertEquals(job.getName(), secondExecution.getJobInstance().getJobName());
    }

    @Test
    public void testGetStepExecutionCountAndLastStepExecution() throws Exception {
        job.setRestartable(true);
        StepSupport step = new StepSupport("restartedStep");

        // first execution
        JobExecution firstJobExec = jobRepository.createJobExecution(job.getName(), jobParameters);
        StepExecution firstStepExec = new StepExecution(step.getName(), firstJobExec);
        jobRepository.add(firstStepExec);

        assertEquals(1, jobRepository.getStepExecutionCount(firstJobExec.getJobInstance(), step.getName()));
        assertEquals(firstStepExec, jobRepository.getLastStepExecution(firstJobExec.getJobInstance(), step.getName()));

        // first execution failed
        firstJobExec.setStartTime(new Date(4));
        firstStepExec.setStartTime(new Date(5));
        firstStepExec.setStatus(BatchStatus.FAILED);
        firstStepExec.setEndTime(new Date(6));
        jobRepository.update(firstStepExec);
        firstJobExec.setStatus(BatchStatus.FAILED);
        firstJobExec.setEndTime(new Date(7));
        jobRepository.update(firstJobExec);

        // second execution
        JobExecution secondJobExec = jobRepository.createJobExecution(job.getName(), jobParameters);
        StepExecution secondStepExec = new StepExecution(step.getName(), secondJobExec);
        jobRepository.add(secondStepExec);

        assertEquals(2, jobRepository.getStepExecutionCount(secondJobExec.getJobInstance(), step.getName()));
        assertEquals(secondStepExec, jobRepository.getLastStepExecution(secondJobExec.getJobInstance(), step.getName()));
    }

    @Test
    public void testSaveExecutionContext() throws Exception {
        ExecutionContext ctx = new ExecutionContext();
        ctx.putLong("crashedPosition", 7);
        JobExecution jobExec = jobRepository.createJobExecution(job.getName(), jobParameters);
        jobExec.setStartTime(new Date(0));
        jobExec.setExecutionContext(ctx);
        Step step = new StepSupport("step1");
        StepExecution stepExec = new StepExecution(step.getName(), jobExec);
        stepExec.setExecutionContext(ctx);

        jobRepository.add(stepExec);

        StepExecution retrievedStepExec = jobRepository.getLastStepExecution(jobExec.getJobInstance(), step.getName());
        assertEquals(stepExec, retrievedStepExec);
        assertEquals(ctx, retrievedStepExec.getExecutionContext());

    }

//    @Test
    public void testOnlyOneJobExecutionAllowedRunning() throws Exception {
        job.setRestartable(true);
        jobRepository.createJobExecution(job.getName(), jobParameters);

        try {
            jobRepository.createJobExecution(job.getName(), jobParameters);
            fail();
        }
        catch (JobExecutionAlreadyRunningException e) {
            // expected
        }
    }

    @Test
    public void testGetLastJobExecution() throws Exception {
        JobExecution jobExecution = jobRepository.createJobExecution(job.getName(), jobParameters);
        jobExecution.setStatus(BatchStatus.FAILED);
        jobExecution.setEndTime(new Date());
        jobRepository.update(jobExecution);
        Thread.sleep(10);
        jobExecution = jobRepository.createJobExecution(job.getName(), jobParameters);
        assertEquals(jobExecution, jobRepository.getLastJobExecution(job.getName(), jobParameters));
    }

}
