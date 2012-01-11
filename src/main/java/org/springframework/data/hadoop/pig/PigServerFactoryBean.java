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
import java.io.InputStream;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Factory for creating a {@link PigServer} instance. Note that since PigServer is not thread-safe, this factory will
 * create a new instance for every {@link #getObject()} invocation. The caller needs to handle the object clean-up, 
 * specifically calling {@link PigServer#shutdown()}.
 * 
 * @author Costin Leau
 */
public class PigServerFactoryBean implements FactoryBean<PigServer>, BeanNameAware {

	private static final Log log = LogFactory.getLog(PigServerFactoryBean.class);

	private PigContext pigContext;
	private Collection<String> pathToSkip;
	private Collection<PigScript> scripts;
	private Integer parallelism;
	private String jobName;
	private String jobPriority;
	private Boolean validateEachStatement;
	private String beanName;

	public PigServer getObject() throws Exception {
		return createPigInstance();
	}

	public Class<?> getObjectType() {
		return PigServer.class;
	}

	public boolean isSingleton() {
		return false;
	}

	protected PigServer createPigInstance() throws IOException {
		PigContext ctx = (pigContext != null ? pigContext : new PigContext());

		// apparently if not connected, pig can cause all kind of errors
		PigServer pigServer = new PigServer(ctx, true);

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


		if (!CollectionUtils.isEmpty(scripts)) {
			for (PigScript script : scripts) {
				InputStream in = null;
				try {
					in = script.getResource().getInputStream();
					pigServer.registerScript(in, script.getArguments());
				} catch (IOException ex) {
					throw new BeanInitializationException("Cannot register script " + script, ex);
				} finally {
					if (in != null) {
						try {
							in.close();
						} catch (IOException ex) {
						}
					}
				}
			}
		}

		return pigServer;
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
	public void setScripts(Collection<PigScript> scripts) {
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