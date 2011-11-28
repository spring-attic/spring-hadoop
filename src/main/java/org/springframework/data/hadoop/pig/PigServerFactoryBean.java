/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.hadoop.pig;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.io.Resource;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Factory for creating a {@link PigServer} instance.
 * 
 * @author Costin Leau
 */
public class PigServerFactoryBean implements SmartLifecycle, InitializingBean, DisposableBean, FactoryBean<PigServer>,
		BeanNameAware {

	private static final Log log = LogFactory.getLog(PigServerFactoryBean.class);

	private PigServer pigServer;
	private volatile boolean running = false;

	private boolean autoStartup = true;

	private PigContext pigContext;
	private Collection<String> pathToSkip;
	private Collection<Resource> scripts;
	private Integer parallelism;
	private String jobName;
	private String jobPriority;
	private Boolean validateEachStatement;
	private String beanName;

	public PigServer getObject() throws Exception {
		return pigServer;
	}

	public Class<?> getObjectType() {
		return (pigServer != null ? pigServer.getClass() : PigServer.class);
	}

	public boolean isSingleton() {
		return true;
	}

	public void afterPropertiesSet() throws Exception {
		PigContext ctx = (pigContext != null ? pigContext : new PigContext());
		pigServer = new PigServer(ctx, false);

		if (!CollectionUtils.isEmpty(pathToSkip)) {
			for (String path : pathToSkip) {
				pigServer.addPathToSkip(path);
			}
		}

		if (parallelism != null) {
			pigServer.setDefaultParallel(parallelism);
		}

		if (StringUtils.hasText(jobName)) {
			pigServer.setJobName(jobName);
		}
		else {
			if (StringUtils.hasText(beanName)) {
				pigServer.setJobName(beanName);
			}
		}

		if (StringUtils.hasText(jobPriority)) {
			pigServer.setJobPriority(jobPriority);
		}

		if (validateEachStatement != null) {
			pigServer.setValidateEachStatement(validateEachStatement);
		}
	}

	public void destroy() throws Exception {
		stop();
	}


	public void setAutoStartup(boolean autoStart) {
		this.autoStartup = autoStart;
	}

	public boolean isRunning() {
		return running;
	}

	public void start() {
		if (!isRunning()) {
			running = true;
			try {
				pigServer.getPigContext().connect();
				registerScripts();
			} catch (Exception ex) {
				throw new IllegalStateException("Cannot start PigServer", ex);
			}
		}
	}

	private void registerScripts() throws IOException {
		if (!CollectionUtils.isEmpty(scripts)) {
			for (Resource resource : scripts) {
				pigServer.registerScript(resource.getInputStream());
			}
		}
	}

	public void stop() {
		if (isRunning()) {
			running = false;
			pigServer.shutdown();
		}
	}

	public boolean isAutoStartup() {
		return autoStartup;
	}

	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	public int getPhase() {
		return Integer.MIN_VALUE;
	}

	public void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * @param pigContext The pigContext to set.
	 */
	public void setPigContext(PigContext pigContext) {
		this.pigContext = pigContext;
	}

	/**
	 * @param pathToSkip The pathToSkip to set.
	 */
	public void setPathsToSkip(Collection<String> pathToSkip) {
		this.pathToSkip = pathToSkip;
	}

	/**
	 * @param scripts The scripts to set.
	 */
	public void setScripts(Collection<Resource> scripts) {
		this.scripts = scripts;
	}

	/**
	 * @param parallelism The parallelism to set.
	 */
	public void setParallelism(Integer parallelism) {
		this.parallelism = parallelism;
	}

	/**
	 * @param jobName The jobName to set.
	 */
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	/**
	 * @param jobPriority The jobPriority to set.
	 */
	public void setJobPriority(String jobPriority) {
		this.jobPriority = jobPriority;
	}

	/**
	 * @param validateEachStatement The validateEachStatement to set.
	 */
	public void setValidateEachStatement(Boolean validateEachStatement) {
		this.validateEachStatement = validateEachStatement;
	}
}