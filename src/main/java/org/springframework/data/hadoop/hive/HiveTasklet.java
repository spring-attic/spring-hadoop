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
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Hive tasklet running Hive scripts on demand, against a {@link HiveClient}.
 * 
 * @author Costin Leau
 */
public class HiveTasklet implements InitializingBean, BeanFactoryAware, Tasklet {

	private HiveClient hive;
	private Collection<HiveScript> scripts;
	private BeanFactory beanFactory;
	private String hiveClientName;

	@Override
	public void afterPropertiesSet() {
		Assert.isTrue(hive != null || StringUtils.hasText(hiveClientName),
				"A HiveClient instance or bean name is required");

		Assert.notEmpty(scripts, "At least one script needs to be specified");

		if (StringUtils.hasText(hiveClientName)) {
			Assert.notNull(beanFactory, "a bean factory is required if the job is specified by name");
			Assert.isTrue(beanFactory.containsBean(hiveClientName), "beanFactory does not contain any bean named ["
					+ hiveClientName + "]");
		}
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		HiveClient h = (hive != null ? hive : beanFactory.getBean(hiveClientName, HiveClient.class));

		try {
			for (HiveScript script : scripts) {
				HiveScriptRunner.run(h, script);
			}
			return RepeatStatus.FINISHED;
		} finally {
			if (hive == null) {
				h.shutdown();
			}
		}
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
	public void setHiveClient(HiveClient hive) {
		this.hive = hive;
	}

	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	/**
	 * Sets the HiveClient to use, by (bean) name. This is the default
	 * method used by the hdp name space to allow lazy initialization and potential scoping
	 * to kick in.
	 * 
	 * @param hiveClientName The HiveClient to use.
	 */
	public void setHiveClientName(String hiveClientName) {
		this.hiveClientName = hiveClientName;
	}
}