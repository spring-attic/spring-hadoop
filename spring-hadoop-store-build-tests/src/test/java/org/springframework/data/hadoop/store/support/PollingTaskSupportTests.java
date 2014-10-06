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
package org.springframework.data.hadoop.store.support;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;

/**
 * Tests for {@code PollingTaskSupport}.
 *
 * @author Janne Valkealahti
 *
 */
public class PollingTaskSupportTests {

	@Test
	public void testPollingTask() throws InterruptedException {
		TaskScheduler taskScheduler = new ConcurrentTaskScheduler();
		TaskExecutor taskExecutor = new SyncTaskExecutor();
		TestPollingTaskSupport poller = new TestPollingTaskSupport(taskScheduler, taskExecutor,
				TimeUnit.SECONDS, 2);
		poller.init();
		poller.start();
		Thread.sleep(3000);
		poller.stop();
		assertThat(poller.counter, is(2));
		Thread.sleep(1000);
		assertThat(poller.counter, is(2));
	}

	@Test
	public void testIdleTimeoutTrigger() throws InterruptedException {

		// if these tests starts to fail or cause other timing
		// problems, we need to find a better way to run
		// test without adding too long sleeps

		TaskScheduler taskScheduler = new ConcurrentTaskScheduler();
		TaskExecutor taskExecutor = new ConcurrentTaskExecutor();
		TestPollingTaskSupport poller = new TestPollingTaskSupport(taskScheduler, taskExecutor);
		IdleTimeoutTrigger trigger = new IdleTimeoutTrigger(1000);
		poller.setTrigger(trigger);
		poller.init();
		poller.start();

		Thread.sleep(600);
		// after 600ms initial no delay, should be 1
		assertThat(poller.counter, is(1));
		Thread.sleep(500);
		// after 1100ms, should be 2
		assertThat(poller.counter, is(2));

		Thread.sleep(500);
		// after 1600ms, should be 2
		assertThat(poller.counter, is(2));
		Thread.sleep(500);
		// after 2100ms, should be 3
		assertThat(poller.counter, is(3));

		Thread.sleep(500);
		// trigger reset should cause different trigger time after netext one
		trigger.reset();
		// after 2600ms, should be 3
		assertThat(poller.counter, is(3));
		Thread.sleep(500);
		// after 3100ms, should be 4
		assertThat(poller.counter, is(4));

		Thread.sleep(600);
		// after 3700ms, should be 5
		assertThat(poller.counter, is(5));
		poller.stop();
		assertThat(poller.counter, is(5));
	}

	private static class TestPollingTaskSupport extends PollingTaskSupport<String> {

		public TestPollingTaskSupport(TaskScheduler taskScheduler, TaskExecutor taskExecutor) {
			super(taskScheduler, taskExecutor);
		}

		public TestPollingTaskSupport(TaskScheduler taskScheduler, TaskExecutor taskExecutor, TimeUnit unit,
				long duration) {
			super(taskScheduler, taskExecutor, unit, duration);
		}

		int counter = 0;

		@Override
		protected String doPoll() {
			return Integer.toString(counter++);
		}

		@Override
		protected void onPollResult(String result) {
		}

	}

}
