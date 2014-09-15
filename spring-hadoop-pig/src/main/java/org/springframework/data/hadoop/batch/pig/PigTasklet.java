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
package org.springframework.data.hadoop.batch.pig;

import java.util.List;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.hadoop.pig.PigExecutor;
import org.springframework.util.CollectionUtils;

/**
 * Pig tasklet. Note the same {@link PigServer} is shared between invocations. 
 * 
 * @author Costin Leau
 */
public class PigTasklet extends PigExecutor implements Tasklet {

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		List<ExecJob> execs = executePigScripts();

		// save stats
		saveStats(execs, contribution);

		return RepeatStatus.FINISHED;
	}

	private void saveStats(List<ExecJob> execs, StepContribution contribution) throws Exception {

		if (CollectionUtils.isEmpty(execs) || contribution == null) {
			return;
		}

		for (ExecJob execJob : execs) {
			PigStats stats = execJob.getStatistics();

			// embedded pig contains no stats and further more throws Exceptions
			// use CDH3 compatible comparison
			if (stats != null && !stats.getClass().getName().contains("EmbeddedPigStats")) {
				// compute the input stats manually
				List<InputStats> inputStats = stats.getInputStats();

				for (InputStats is : inputStats) {
					for (int i = 0; i < safeLongToInt(is.getNumberRecords()); i++) {
						contribution.incrementReadCount();
					}
				}

				contribution.incrementWriteCount(safeLongToInt(stats.getRecordWritten()));

				// Skip information not available yet
				// workaround: query the internal map/reduce jobs ?
				//contribution.incrementReadSkipCount(safeLongToInt(count.getValue()));
				//
				//for (int i = 0; i < safeLongToInt(count.getValue()); i++) {
				//	contribution.incrementWriteSkipCount();
				//}
			}
		}
	}

	static int safeLongToInt(long l) {
		if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
			throw new IllegalArgumentException(l + " cannot be cast to int without changing its value.");
		}
		return (int) l;
	}
}