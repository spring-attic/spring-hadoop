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
package org.springframework.yarn.test.junit;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.test.context.YarnCluster;

/**
 * Abstract base class providing default functionality
 * for running tests using Yarn mini cluster.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
public abstract class AbstractYarnClusterTests implements ApplicationContextAware {

	protected ApplicationContext applicationContext;

	protected Configuration configuration;

	protected YarnCluster yarnCluster;

	protected YarnClient yarnClient;

	/**
	 * Gets the {@link ApplicationContext} for tests.
	 *
	 * @return the Application context
	 */
	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	/**
	 * Gets the running cluster runtime
	 * {@link Configuration} for tests.
	 *
	 * @return the Yarn cluster config
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Sets the {@link Configuration}.
	 *
	 * @param configuration the Configuration
	 */
	@Autowired
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Gets the running {@link YarnCluster} for tests.
	 *
	 * @return the Yarn cluster
	 */
	public YarnCluster getYarnCluster() {
		return yarnCluster;
	}

	/**
	 * Sets the {@link YarnCluster}
	 *
	 * @param yarnCluster the Yarn cluster
	 */
	@Autowired
	public void setYarnCluster(YarnCluster yarnCluster) {
		this.yarnCluster = yarnCluster;
	}

	/**
	 * Gets the {@link YarnClient}.
	 *
	 * @return the Yarn client
	 */
	public YarnClient getYarnClient() {
		return yarnClient;
	}

	/**
	 * Sets the {@link YarnClient}.
	 *
	 * @param yarnClient the Yarn client
	 */
	@Autowired
	public void setYarnClient(YarnClient yarnClient) {
		this.yarnClient = yarnClient;
	}

	/**
	 * Submits application and wait state. On default
	 * waits 60 seconds.
	 *
	 * @return Application info for submit
	 * @throws Exception if exception occurred
	 * @see ApplicationInfo
	 * @see #submitApplicationAndWaitState(long, TimeUnit, YarnApplicationState...)
	 */
	protected ApplicationInfo submitApplicationAndWait() throws Exception {
		return submitApplicationAndWait(60, TimeUnit.SECONDS);
	}

	/**
	 * Submits application and wait state.
	 *
	 * @param timeout the timeout for wait
	 * @param unit the unit for timeout
	 * @return Application info for submit
	 * @throws Exception if exception occurred
	 * @see ApplicationInfo
	 * @see #submitApplicationAndWaitState(long, TimeUnit, YarnApplicationState...)
	 */
	protected ApplicationInfo submitApplicationAndWait(long timeout, TimeUnit unit) throws Exception {
		return submitApplicationAndWaitState(timeout, unit, YarnApplicationState.FINISHED, YarnApplicationState.FAILED);
	}

	/**
	 * Submits application and wait state. Returned state is <code>NULL</code>
	 * if something failed or final known state after the wait/poll operations.
	 * Array of application states can be used to return immediately from wait
	 * loop if state is matched.
	 *
	 * @param timeout the timeout for wait
	 * @param unit the unit for timeout
	 * @param applicationStates the application states to wait
	 * @return Application info for submit
	 * @throws Exception if exception occurred
	 * @see ApplicationInfo
	 */
	protected ApplicationInfo submitApplicationAndWaitState(long timeout, TimeUnit unit, YarnApplicationState... applicationStates) throws Exception {
		Assert.notEmpty(applicationStates, "Need to have atleast one state");
		Assert.notNull(getYarnClient(), "Yarn client must be set");

		YarnApplicationState state = null;
		ApplicationReport report = null;

		ApplicationId applicationId = submitApplication();
		Assert.notNull(applicationId, "Failed to get application id from submit");

		long end = System.currentTimeMillis() + unit.toMillis(timeout);

		// break label for inner loop
		done:
		do {
			report = findApplicationReport(getYarnClient(), applicationId);
			if (report == null) {
				break;
			}
			state = report.getYarnApplicationState();
			for (YarnApplicationState stateCheck : applicationStates) {
				if (state.equals(stateCheck)) {
					break done;
				}
			}
			Thread.sleep(1000);
		} while (System.currentTimeMillis() < end);
		return new ApplicationInfo(applicationId, report);
	}

	/**
	 * Submit an application.
	 *
	 * @return the submitted application {@link ApplicationId}
	 */
	protected ApplicationId submitApplication() {
		Assert.notNull(getYarnClient(), "Yarn client must be set");
		ApplicationId applicationId = getYarnClient().submitApplication();
		Assert.notNull(applicationId, "Failed to get application id from submit");
		return applicationId;
	}

	/**
	 * Waits state. Returned state is <code>NULL</code>
	 * if something failed or final known state after the wait/poll operations.
	 * Array of application states can be used to return immediately from wait
	 * loop if state is matched.
	 *
	 * @param applicationId the application id
	 * @param timeout the timeout for wait
	 * @param unit the unit for timeout
	 * @param applicationStates the application states to wait
	 * @return Last known application state or <code>NULL</code> if timeout
	 * @throws Exception if exception occurred
	 */
	protected YarnApplicationState waitState(ApplicationId applicationId, long timeout, TimeUnit unit, YarnApplicationState... applicationStates) throws Exception {
		Assert.notNull(getYarnClient(), "Yarn client must be set");
		Assert.notNull(applicationId, "ApplicationId must not be null");

		YarnApplicationState state = null;
		long end = System.currentTimeMillis() + unit.toMillis(timeout);

		// break label for inner loop
		done:
		do {
			state = findState(getYarnClient(), applicationId);
			if (state == null) {
				break;
			}
			for (YarnApplicationState stateCheck : applicationStates) {
				if (state.equals(stateCheck)) {
					break done;
				}
			}
			Thread.sleep(1000);
		} while (System.currentTimeMillis() < end);
		return state;
	}

	/**
	 * Kill the application.
	 *
	 * @param applicationId the application id
	 */
	protected void killApplication(ApplicationId applicationId) {
		Assert.notNull(getYarnClient(), "Yarn client must be set");
		Assert.notNull(applicationId, "ApplicationId must not be null");
		getYarnClient().killApplication(applicationId);
	}

	/**
	 * Get the current application state.
	 *
	 * @param applicationId Yarn app application id
	 * @return Current application state or <code>NULL</code> if not found
	 */
	protected YarnApplicationState getState(ApplicationId applicationId) {
		Assert.notNull(getYarnClient(), "Yarn client must be set");
		Assert.notNull(applicationId, "ApplicationId must not be null");
		return findState(getYarnClient(), applicationId);
	}

	/**
	 * Finds the current application state.
	 *
	 * @param client the Yarn client
	 * @param applicationId Yarn app application id
	 * @return Current application state or <code>NULL</code> if not found
	 */
	private YarnApplicationState findState(YarnClient client, ApplicationId applicationId) {
		YarnApplicationState state = null;
		for (ApplicationReport report : client.listApplications()) {
			if (report.getApplicationId().equals(applicationId)) {
				state = report.getYarnApplicationState();
				break;
			}
		}
		return state;
	}

	/**
	 * Finds the current application report.
	 *
	 * @param client the Yarn client
	 * @param applicationId Yarn app application id
	 * @return Current application report or <code>NULL</code> if not found
	 */
	private ApplicationReport findApplicationReport(YarnClient client, ApplicationId applicationId) {
		for (ApplicationReport report : client.listApplications()) {
			if (report.getApplicationId().equals(applicationId)) {
				return report;
			}
		}
		return null;
	}

}
