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
package org.springframework.yarn.boot.support;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.AppmasterConstants;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.listener.AppmasterStateListener;

/**
 * {@link CommandLineRunner} to {@link YarnAppmaster run} Spring Yarn appmaster.
 *
 * @author Janne Valkealahti
 *
 */
public class AppmasterLauncherRunner implements CommandLineRunner {

	private static final Log log = LogFactory.getLog(AppmasterLauncherRunner.class);

	/** Latch used for appmaster wait */
	private CountDownLatch latch = new CountDownLatch(1);

	private boolean waitLatch = true;

	private int containerCount = 1;

	@Autowired(required = false)
	private YarnAppmaster yarnAppmaster;

	@Override
	public void run(String... args) throws Exception {
		if (yarnAppmaster != null) {
			launchAppmaster(yarnAppmaster, args);
		}
	}

	public void setWaitLatch(boolean waitLatch) {
		this.waitLatch = waitLatch;
	}

	public void setContainerCount(int containerCount) {
		this.containerCount = containerCount;
	}

	protected void launchAppmaster(YarnAppmaster appmaster, String[] parameters) {
		Properties properties = StringUtils.splitArrayElementsIntoProperties(parameters, "=");
		if (properties == null) {
			properties = new Properties();
		}

		if (!properties.containsKey(AppmasterConstants.CONTAINER_COUNT)) {
			log.info("Setting container count set externally " + containerCount);
			properties.put(AppmasterConstants.CONTAINER_COUNT, Integer.toString(containerCount));
		}
		appmaster.setParameters(properties);

//		appmaster.setParameters(properties != null ? properties : new Properties());


		log.info("Running YarnAppmaster with parameters [" + StringUtils.arrayToCommaDelimitedString(parameters) + "]");

		appmaster.addAppmasterStateListener(new AppmasterStateListener() {
			@Override
			public void state(AppmasterState state) {
				if(state == AppmasterState.COMPLETED) {
					latch.countDown();
				}
			}
		});

		appmaster.submitApplication();

		if (waitLatch) {
			log.info("Waiting latch to receive appmaster complete state");
			try {
				latch.await();
			} catch (InterruptedException e) {
				log.info("YarnAppmaster latch wait interrupted");
			}

			log.info("YarnAppmaster complete");
			// TODO: think how to handle exit from a boot app
			System.exit(0);
		}
	}

}
