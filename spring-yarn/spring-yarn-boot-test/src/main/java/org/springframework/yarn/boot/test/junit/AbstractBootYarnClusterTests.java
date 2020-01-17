/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.yarn.boot.test.junit;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.yarn.boot.SpringApplicationCallback;
import org.springframework.yarn.boot.SpringApplicationTemplate;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.test.junit.AbstractYarnClusterTests;
import org.springframework.yarn.test.junit.ApplicationInfo;

/**
 * Abstract base class providing default functionality for running tests for
 * Spring Yarn Boot based apps using Yarn mini cluster.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractBootYarnClusterTests extends AbstractYarnClusterTests {

	/**
	 * Empty Spring @{@link org.springframework.context.annotation.Configuration}
	 * class which can be referenced from tests solely using JavaConfig.
	 * If tests are based on Spring test support, at minimum a dummy
	 * empty class needs to be provided if xml configs are not present.
	 */
	@org.springframework.context.annotation.Configuration
	public static class EmptyConfig {
	}

	@Override
	public void setYarnClient(YarnClient yarnClient) {
		// override here for autowiring not to fail
		// in super. We set client later in this class.
		super.setYarnClient(yarnClient);
	}

	/**
	 * Submits application and wait state. On default
	 * waits 60 seconds.
	 *
	 * @param source the boot application config source
	 * @param args the boot application args
	 * @return Application info for submit
	 * @throws Exception if exception occurred
	 * @see ApplicationInfo
	 * @see #submitApplicationAndWaitState(Class , String[], long, TimeUnit, YarnApplicationState...)
	 */
	protected ApplicationInfo submitApplicationAndWait(Class<?> source, String[] args) throws Exception {
		return submitApplicationAndWait(source, args, 60, TimeUnit.SECONDS);
	}

	/**
	 * Submits application and wait state.
	 *
	 * @param source the boot application config source
	 * @param args the boot application args
	 * @param timeout the timeout for wait
	 * @param unit the unit for timeout
	 * @return Application info for submit
	 * @throws Exception if exception occurred
	 * @see ApplicationInfo
	 * @see #submitApplicationAndWaitState(Class, String[], long, TimeUnit, YarnApplicationState...)
	 */
	protected ApplicationInfo submitApplicationAndWait(Class<?> source, String[] args, long timeout, final TimeUnit unit) throws Exception {
		return submitApplicationAndWaitState(source, args, timeout, unit, YarnApplicationState.FINISHED, YarnApplicationState.FAILED);
	}

	/**
	 * Submits application and wait state. Returned state is <code>NULL</code>
	 * if something failed or final known state after the wait/poll operations.
	 * Array of application states can be used to return immediately from wait
	 * loop if state is matched.
	 *
	 * @param source the boot application config source
	 * @param args the boot application args
	 * @param timeout the timeout for wait
	 * @param unit the unit for timeout
	 * @param applicationStates the application states to wait
	 * @return Application info for submit
	 * @throws Exception if exception occurred
	 * @see ApplicationInfo
	 */
	protected ApplicationInfo submitApplicationAndWaitState(Class<?> source, String[] args, final long timeout,
			final TimeUnit unit, final YarnApplicationState... applicationStates) throws Exception {

		SpringApplicationBuilder builder = new SpringApplicationBuilder(source);
		builder.initializers(new HadoopConfigurationInjectingInitializer(getConfiguration()));

		SpringApplicationTemplate template = new SpringApplicationTemplate(builder);
		return template.execute(new SpringApplicationCallback<ApplicationInfo>() {

			@Override
			public ApplicationInfo runWithSpringApplication(ApplicationContext context) throws Exception {
				setYarnClient(context.getBean(YarnClient.class));
				return submitApplicationAndWaitState(timeout, unit, applicationStates);
			}

		}, args);
	}

	/**
	 * Context initializer which registers Hadoop's Configuration into bean
	 * factory. This trick is needed because mini cluster is running
	 * on a same context as tests but Boot application will have its
	 * own context where we need to pass this on.
	 */
	private static class HadoopConfigurationInjectingInitializer implements
			ApplicationContextInitializer<ConfigurableApplicationContext> {

		private final Configuration configuration;

		public HadoopConfigurationInjectingInitializer(Configuration configuration) {
			this.configuration = configuration;
		}

		@Override
		public void initialize(ConfigurableApplicationContext applicationContext) {
			applicationContext.getBeanFactory().registerSingleton("miniYarnConfiguration", configuration);
		}

	}

}
