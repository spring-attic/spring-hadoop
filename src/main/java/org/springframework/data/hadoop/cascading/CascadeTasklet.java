/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.cascading;

import org.apache.hadoop.mapred.Task;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import cascading.cascade.Cascade;
import cascading.management.UnitOfWork;
import cascading.stats.CascadeStats;

/**
 * Batch tasklet for executing a {@link Cascade} as part of a job.
 * 
 * @author Costin Leau
 */
public class CascadeTasklet implements Tasklet {

	private UnitOfWork<? extends CascadeStats> unitOfWork;
	private boolean waitToComplete = true;

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		System.out.println("Before execution " + contribution);
		CascadeStats stats = Runner.run(unitOfWork, waitToComplete);

		// save stats
		for (int i = 0; i < safeLongToInt(stats.getCounterValue(Task.Counter.MAP_INPUT_BYTES)); i++) {
			contribution.incrementReadCount();
		}

		contribution.incrementReadSkipCount(safeLongToInt(stats.getCounterValue(Task.Counter.MAP_SKIPPED_RECORDS)));
		contribution.incrementWriteCount(safeLongToInt(stats.getCounterValue(Task.Counter.REDUCE_OUTPUT_RECORDS)));

		for (int i = 0; i < safeLongToInt(stats.getCounterValue(Task.Counter.REDUCE_SKIPPED_RECORDS)); i++) {
			contribution.incrementWriteSkipCount();
		}

		System.out.println("After execution " + contribution);
		return RepeatStatus.FINISHED;
	}

	static int safeLongToInt(long l) {
		if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
			throw new IllegalArgumentException(l + " cannot be cast to int without changing its value.");
		}
		return (int) l;
	}


	/**
	 * Sets the unit of work or a {@link Cascade}.
	 *
	 * @param cascade the new cascade
	 */
	public void setUnitOfWork(Cascade cascade) {
		this.unitOfWork = cascade;
	}
}