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
import java.security.PrivilegedExceptionAction;
import java.util.Collection;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
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

	private PigContext pigContext;
	private Collection<String> pathToSkip;
	private Collection<PigScript> scripts;
	private Integer parallelism;
	private String jobName;
	private String jobPriority;
	private Boolean validateEachStatement;
	private String beanName;

	private String user;

	public PigServer getObject() throws Exception {
		return createPigInstance();
	}

	public Class<?> getObjectType() {
		return PigServer.class;
	}

	public boolean isSingleton() {
		return false;
	}

	protected PigServer createPigInstance() throws Exception {
		final PigContext ctx = (pigContext != null ? pigContext : new PigContext());

		// apparently if not connected, pig can cause all kind of errors
		PigServer pigServer = null;

		if (StringUtils.hasText(user)) {
			UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
			pigServer = ugi.doAs(new PrivilegedExceptionAction<PigServer>() {
				@Override
				public PigServer run() throws Exception {
					return new PigServer(ctx, true);
				}
			});
		}
		else {
			pigServer = new PigServer(ctx, true);
		}

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
					IOUtils.closeStream(in);
				}
			}
		}

		return pigServer;
	}

	public void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Sets the {@link PigContext} to use.
	 * 
	 * @param pigContext The pigContext to set.
	 */
	public void setPigContext(PigContext pigContext) {
		this.pigContext = pigContext;
	}

	/**
	 * Sets the paths to skip.
	 * 
	 * @param pathToSkip The pathToSkip to set.
	 */
	public void setPathsToSkip(Collection<String> pathToSkip) {
		this.pathToSkip = pathToSkip;
	}

	/**
	 * Sets the scripts to execute at startup.
	 * 
	 * @param scripts The scripts to set.
	 */
	public void setScripts(Collection<PigScript> scripts) {
		this.scripts = scripts;
	}

	/**
	 * Sets the parallelism.
	 * 
	 * @param parallelism The parallelism to set.
	 */
	public void setParallelism(Integer parallelism) {
		this.parallelism = parallelism;
	}

	/**
	 * Sets the job name.
	 * 
	 * @param jobName The jobName to set.
	 */
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	/**
	 * Sets the job priority.
	 * 
	 * @param jobPriority The jobPriority to set.
	 */
	public void setJobPriority(String jobPriority) {
		this.jobPriority = jobPriority;
	}

	/**
	 * Indicates whether each statement should be validated or not.
	 * 
	 * @param validateEachStatement whether to validate each statement or not.
	 */
	public void setValidateEachStatement(Boolean validateEachStatement) {
		this.validateEachStatement = validateEachStatement;
	}

	/**
	 * Sets the user impersonation (optional) for executing Pig jobs.
	 * Should be used when running against a Hadoop Kerberos cluster. 
	 * 
	 * @param user user/group information
	 */
	public void setUser(String user) {
		this.user = user;
	}
}