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
package org.springframework.data.hadoop.hive;

import java.util.Collection;

import org.apache.hadoop.hive.service.HiveClient;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.util.Assert;

/**
 * Hive tasklet running Hive scripts on demand, against a {@link HiveClient}.
 * 
 * @author Costin Leau
 */
public class HiveTasklet implements InitializingBean, Tasklet {

	private ObjectFactory<HiveClient> hiveClientFactory;
	private HiveTemplate hiveTemplate;
	private Collection<HiveScript> scripts;

	@Override
	public void afterPropertiesSet() {
		if (hiveClientFactory == null && hiveTemplate == null) {
			throw new IllegalArgumentException("a HiveClient factory or a HiveTemplate is required");
		}

		if (hiveTemplate == null) {
			hiveTemplate = new HiveTemplate(hiveClientFactory);
		}

		Assert.notEmpty(scripts, "no scripts specified");
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		hiveTemplate.execute(scripts);
		return RepeatStatus.FINISHED;
	}

	/**
	 * Sets the scripts to be executed by this tasklet.
	 * 
	 * @param scripts The scripts to set.
	 */
	public void setScripts(Collection<HiveScript> scripts) {
		this.scripts = scripts;
	}

	/**
	 * Sets the hive client for this tasklet.
	 *  
	 * @param hive HiveClient to set
	 */
	public void setHiveClient(ObjectFactory<HiveClient> hiveFactory) {
		this.hiveClientFactory = hiveFactory;
	}

	/**
	 * Sets the hive template.
	 *
	 * @param hiveTemplate the new hive template
	 */
	public void setHiveTemplate(HiveTemplate hiveTemplate) {
		this.hiveTemplate = hiveTemplate;
	}
}