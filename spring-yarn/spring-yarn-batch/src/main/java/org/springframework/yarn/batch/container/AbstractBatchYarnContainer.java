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
package org.springframework.yarn.batch.container;

import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.step.StepLocator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.container.YarnContainer;
import org.springframework.yarn.integration.container.AbstractIntegrationYarnContainer;

/**
 * Base implementation of {@link YarnContainer} extending
 * {@link AbstractIntegrationYarnContainer} adding functionality
 * for Spring Batch.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractBatchYarnContainer extends AbstractIntegrationYarnContainer {

	/** The step locator. */
	private StepLocator stepLocator;

	/** The job explorer. */
	private JobExplorer jobExplorer;

	/**
	 * Gets the step locator.
	 *
	 * @return the step locator
	 */
	public StepLocator getStepLocator() {
		return stepLocator;
	}

	/**
	 * Sets the step locator.
	 *
	 * @param stepLocator the new step locator
	 */
	@Autowired(required=false)
	public void setStepLocator(StepLocator stepLocator) {
		this.stepLocator = stepLocator;
	}

	/**
	 * Gets the job explorer.
	 *
	 * @return the job explorer
	 */
	public JobExplorer getJobExplorer() {
		return jobExplorer;
	}

	/**
	 * Sets the job explorer.
	 *
	 * @param jobExplorer the new job explorer
	 */
	@Autowired(required=false)
	public void setJobExplorer(JobExplorer jobExplorer) {
		this.jobExplorer = jobExplorer;
	}

}
