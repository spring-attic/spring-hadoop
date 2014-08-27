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
package org.springframework.yarn.batch.support;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.yarn.batch.support.YarnBatchProperties.JobProperties;

/**
 * Tests for {@link YarnJobLauncher}.
 *
 * @author Janne Valkealahti
 */
public class YarnJobLauncherTests {

	private YarnJobLauncher runner;

	private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

	private JobExplorer jobExplorer;

	private JobLauncher jobLauncher;

	private JobBuilderFactory jobs;

	private StepBuilderFactory steps;

	private Job job;

	private Step step;

	@Before
	public void init() throws Exception {
		context.register(BatchConfiguration.class);
		context.refresh();
		JobRepository jobRepository = context.getBean(JobRepository.class);
		jobLauncher = context.getBean(JobLauncher.class);
		jobs = new JobBuilderFactory(jobRepository);
		PlatformTransactionManager transactionManager = this.context.getBean(PlatformTransactionManager.class);
		steps = new StepBuilderFactory(jobRepository, transactionManager);
		step = steps.get("step").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				return null;
			}
		}).build();
		job = jobs.get("job").start(step).build();
		jobExplorer = context.getBean(JobExplorer.class);
		runner = new YarnJobLauncher();
		runner.setJobLauncher(jobLauncher);
		runner.setJobExplorer(jobExplorer);
		context.getBean(BatchConfiguration.class).clear();
	}

	@Test
	public void basicExecution() throws Exception {
		runner.executeJob(job, new JobParameters());
		assertEquals(1, jobExplorer.getJobInstances("job", 0, 100).size());
		runner.executeJob(job, new JobParametersBuilder().addLong("id", 1L).toJobParameters());
		assertEquals(2, jobExplorer.getJobInstances("job", 0, 100).size());
	}

	@Test
	public void incrementExistingExecution() throws Exception {
		YarnBatchProperties yarnBatchProperties = new YarnBatchProperties();
		JobProperties jobProperties = new JobProperties();
		jobProperties.setName("job");
		jobProperties.setNext(true);
		yarnBatchProperties.setJobs(new ArrayList<YarnBatchProperties.JobProperties>(Arrays.asList(jobProperties)));
		runner.setYarnBatchProperties(yarnBatchProperties);

		job = jobs.get("job").start(step).incrementer(new RunIdIncrementer()).build();
		runner.executeJob(job, new JobParameters());
		runner.executeJob(job, new JobParameters());
		assertEquals(2, jobExplorer.getJobInstances("job", 0, 100).size());
	}

	@Test
	public void retryFailedExecution() throws Exception {
		job = jobs.get("job").start(steps.get("step").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				throw new RuntimeException("Planned");
			}
		}).build()).incrementer(new RunIdIncrementer()).build();
		runner.executeJob(job, new JobParameters());
		runner.executeJob(job, new JobParameters());
		assertEquals(1, jobExplorer.getJobInstances("job", 0, 100).size());
	}

	@Test
	public void retryFailedExecutionWithNonIdentifyingParameters() throws Exception {
		job = jobs.get("job").start(this.steps.get("step").tasklet(new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				throw new RuntimeException("Planned");
			}
		}).build()).incrementer(new RunIdIncrementer()).build();
		JobParameters jobParameters = new JobParametersBuilder().addLong("id", 1L, false).toJobParameters();
		runner.executeJob(job, jobParameters);
		runner.executeJob(job, jobParameters);
		assertEquals(1, jobExplorer.getJobInstances("job", 0, 100).size());
	}

	@Configuration
	@EnableBatchProcessing
	protected static class BatchConfiguration implements BatchConfigurer {

		private ResourcelessTransactionManager transactionManager = new ResourcelessTransactionManager();
		private JobRepository jobRepository;
		private MapJobRepositoryFactoryBean jobRepositoryFactory = new MapJobRepositoryFactoryBean(
				this.transactionManager);

		public BatchConfiguration() throws Exception {
			this.jobRepository = this.jobRepositoryFactory.getObject();
		}

		public void clear() {
			this.jobRepositoryFactory.clear();
		}

		@Override
		public JobRepository getJobRepository() throws Exception {
			return this.jobRepository;
		}

		@Override
		public PlatformTransactionManager getTransactionManager() throws Exception {
			return this.transactionManager;
		}

		@Override
		public JobLauncher getJobLauncher() throws Exception {
			SimpleJobLauncher launcher = new SimpleJobLauncher();
			launcher.setJobRepository(this.jobRepository);
			launcher.setTaskExecutor(new SyncTaskExecutor());
			return launcher;
		}

		@Override
		public JobExplorer getJobExplorer() throws Exception {
			return new MapJobExplorerFactoryBean(this.jobRepositoryFactory).getObject();
		}

	}

}
