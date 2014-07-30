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
package org.springframework.data.hadoop.batch;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = HadoopDelegatingSmartContextLoader.class, locations = { "/org/springframework/data/hadoop/batch/multi-thread.xml" })
@MiniHadoopCluster
@DirtiesContext(classMode=ClassMode.AFTER_CLASS)
public class BatchTest {

	public static class ThreadedTasklet implements Tasklet {

		@Override
		public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
			SimpleAsyncTaskExecutor asyncTX = new SimpleAsyncTaskExecutor();

			StepContext context = StepSynchronizationManager.getContext();
			final StepExecution stepExecution = (context != null) ? context.getStepExecution() : null;

			final AtomicBoolean done = new AtomicBoolean(false);

			final Thread[] th = new Thread[1];

			asyncTX.execute(new Runnable() {
				@Override
				public void run() {
					th[0] = Thread.currentThread();
					StepSynchronizationManager.register(stepExecution);
					try {
						Thread.sleep(3000);
						done.set(true);
						Thread.sleep(1000);
					} catch (Exception ex) {
					}
					StepSynchronizationManager.close();
				}
			});

			while (!done.get()) {
				Thread.sleep(400);
				//th[0].join();
			}

			return RepeatStatus.FINISHED;
		}
	}
	
	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testMultiThreadedBatch() throws Exception {
		JobsTrigger.startJobs(ctx);
	}

	@Test
	public void testMultiThreadedBatch2() throws Exception {
		testMultiThreadedBatch();
	}
 }
