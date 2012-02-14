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

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import cascading.cascade.Cascade;
import cascading.flow.Flow;

/**
 * Batch tasklet for executing a {@link Cascade} or a {@link Flow} as part of a job.
 * 
 * @author Costin Leau
 */
public class CascadeTasklet implements InitializingBean, Tasklet {

	private Cascade cascade;
	private Flow flow;
	private boolean waitToComplete = true;

	@Override
	public void afterPropertiesSet() {
		Assert.isTrue(cascade != null || flow != null, "either a flow or cascade need to be specified");
		Assert.isTrue(!(cascade == null || flow == null), "either a flow or cascade need to be specified; not both");
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		if (waitToComplete) {
			if (cascade != null) {
				cascade.complete();
			}
			else {
				flow.complete();
			}
		}

		else {
			if (cascade != null) {
				cascade.start();
			}
			else {
				flow.start();
			}
		}

		return RepeatStatus.FINISHED;
	}

	/**
	 * Sets the cascade.
	 *
	 * @param cascade the new cascade
	 */
	public void setCascade(Cascade cascade) {
		this.cascade = cascade;
	}

	/**
	 * Sets the flow.
	 *
	 * @param flow the new flow
	 */
	public void setFlow(Flow flow) {
		this.flow = flow;
	}
}