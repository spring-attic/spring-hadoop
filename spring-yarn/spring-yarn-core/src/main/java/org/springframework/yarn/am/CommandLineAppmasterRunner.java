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
package org.springframework.yarn.am;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.StringUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.launch.AbstractCommandLineRunner;
import org.springframework.yarn.launch.ExitStatus;
import org.springframework.yarn.listener.AppmasterStateListener;

/**
 * Simple launcher for application master.
 *
 * @author Janne Valkealahti
 *
 */
public class CommandLineAppmasterRunner extends AbstractCommandLineRunner<YarnAppmaster>{

	private static final Log log = LogFactory.getLog(CommandLineAppmasterRunner.class);

	/** Latch to wait appmaster complete state */
	private CountDownLatch latch = new CountDownLatch(1);

	@Override
	protected ExitStatus handleBeanRun(YarnAppmaster bean, String[] parameters, Set<String> opts) {
		Properties properties = StringUtils.splitArrayElementsIntoProperties(parameters, "=");
		bean.setParameters(properties != null ? properties : new Properties());
		bean.setEnvironment(System.getenv());
		if(log.isDebugEnabled()) {
			log.debug("Starting YarnAppmaster bean: " + StringUtils.arrayToCommaDelimitedString(parameters));
		}

		bean.addAppmasterStateListener(new AppmasterStateListener() {
			@Override
			public void state(AppmasterState state) {
				if(state == AppmasterState.COMPLETED) {
					latch.countDown();
				}
			}
		});

		bean.submitApplication();

		if(log.isDebugEnabled()) {
			log.debug("Waiting latch to receive appmaster complete state");
		}
		try {
			latch.await();
		} catch (InterruptedException e) {
			log.debug("Latch interrupted");
		}
		log.info("Bean run completed");
		return ExitStatus.COMPLETED;
	}

	@Override
	protected String getDefaultBeanIdentifier() {
		return YarnSystemConstants.DEFAULT_ID_APPMASTER;
	}

	@Override
	protected List<String> getValidOpts() {
		return null;
	}

	public static void main(String[] args) {
		new CommandLineAppmasterRunner().doMain(args);
	}

}
