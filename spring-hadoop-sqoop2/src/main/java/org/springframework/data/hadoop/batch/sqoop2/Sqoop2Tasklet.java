/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.hadoop.batch.sqoop2;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.client.SubmissionCallback;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.Iterator;

/**
 * Sqoop2 tasklet running Sqoop2 jobs on demand.
 * 
 * @author Thomas Risberg
 */
public class Sqoop2Tasklet implements InitializingBean, Tasklet, StepExecutionListener {

	private String sqoopUrl;

	private Long jobId;

	private Long pollTime = 5000L;

	private boolean complete = false;

	public void setSqoopUrl(String sqoopUrl) {
		this.sqoopUrl = sqoopUrl;
	}

	public void setJobId(Long jobId) {
		this.jobId = jobId;
	}

	public void setPollTime(Long pollTime) {
		this.pollTime = pollTime;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		// TODO
		SqoopClient client = new SqoopClient(sqoopUrl);
		final StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
		client.startJob(jobId,
				new SubmissionCallback() {
					@Override
					public void submitted(MSubmission mSubmission) {
						stepExecution.getExecutionContext().putString("sqoop2.job.id", String.valueOf(mSubmission.getJobId()));
						stepExecution.getExecutionContext().putString("sqoop2.external.job.id", mSubmission.getExternalJobId());
					}

					@Override
					public void updated(MSubmission mSubmission) {

					}

					@Override
					public void finished(MSubmission mSubmission) {
						if (mSubmission.getStatus().isFailure()) {
							stepExecution.getExecutionContext().putString("sqoop2.job.status", mSubmission.getStatus().toString());
						}
						else {
							complete = true;
							stepExecution.getExecutionContext().putString("sqoop2.job.status", mSubmission.getStatus().toString());
							StringBuilder counterInfo = new StringBuilder();
							Counters counters = mSubmission.getCounters();
							Iterator<CounterGroup> cgIter = counters.iterator();
							while (cgIter.hasNext()) {
								CounterGroup cg = cgIter.next();
								counterInfo.append(cg.getName()+":\n");
								Iterator<Counter> cIter = cg.iterator();
								while (cIter.hasNext()) {
									Counter c = cIter.next();
									counterInfo.append("    " + c.getName() + "=" + c.getValue() + "\n");
								}
							}
							stepExecution.getExecutionContext().putString("sqoop2.job.counters", counterInfo.toString());
						}
					}
				}, pollTime);
		return RepeatStatus.FINISHED;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {

	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (complete) {
			return ExitStatus.COMPLETED;
		}
		else {
			return ExitStatus.FAILED;
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.hasLength(sqoopUrl, "The 'sqoopUrl' property must be set");
		Assert.notNull(jobId, "The 'jobId' property must be set");
	}
}