/*
 * Copyright 2013-2016 the original author or authors.
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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.AppmasterConstants;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.launch.ExitStatus;
import org.springframework.yarn.listener.AppmasterStateListener;

/**
 * {@link CommandLineRunner} to {@link YarnAppmaster run} Spring Yarn appmaster.
 *
 * @author Janne Valkealahti
 *
 */
public class AppmasterLauncherRunner extends CommandLineRunnerSupport implements CommandLineRunner {

	private static final Log log = LogFactory.getLog(AppmasterLauncherRunner.class);

	private int containerCount = 1;

	@Autowired(required = false)
	private YarnAppmaster yarnAppmaster;

	private final AtomicBoolean exitCallGuard = new AtomicBoolean();

	@Override
	public void run(String... args) throws Exception {
		if (yarnAppmaster != null) {
			launchAppmaster(yarnAppmaster, args);
		}
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
		appmaster.setEnvironment(System.getenv());


		log.info("Running YarnAppmaster with parameters [" + StringUtils.arrayToCommaDelimitedString(parameters) + "]");

		appmaster.addAppmasterStateListener(new AppmasterStateListener() {
			@Override
			public void state(AppmasterState state) {
				if (log.isDebugEnabled()) {
					log.debug("Received appmaster state " + state);
				}
				if (state == AppmasterState.COMPLETED) {
					countDownLatch();
					if (exitCallGuard.compareAndSet(false, true)) {
						exit(ExitStatus.COMPLETED.getExitCode());
					}
				} else if (state == AppmasterState.FAILED) {
					countDownLatch();
					if (exitCallGuard.compareAndSet(false, true)) {
						exit(ExitStatus.FAILED.getExitCode());
					}
				}
			}
		});

		appmaster.submitApplication();

		if (isWaitLatch()) {
			log.info("Waiting latch to receive appmaster complete state");
			waitLatch();
			log.info("YarnAppmaster complete");
		}
	}

}
