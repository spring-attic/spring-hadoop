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
import org.springframework.yarn.container.LongRunningYarnContainer;
import org.springframework.yarn.container.YarnContainer;
import org.springframework.yarn.launch.ExitCodeMapper;
import org.springframework.yarn.launch.JvmSystemExiter;
import org.springframework.yarn.launch.SimpleJvmExitCodeMapper;
import org.springframework.yarn.launch.SystemExiter;
import org.springframework.yarn.listener.ContainerStateListener;
import org.springframework.yarn.listener.ContainerStateListener.ContainerState;

/**
 * {@link CommandLineRunner} to {@link YarnContainer run} Spring Yarn container.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerLauncherRunner implements CommandLineRunner {

	private static final Log log = LogFactory.getLog(ContainerLauncherRunner.class);

	private static SystemExiter systemExiter = new JvmSystemExiter();

	private ExitCodeMapper exitCodeMapper = new SimpleJvmExitCodeMapper();

	/** Latch used for long running container wait */
	private CountDownLatch latch;

	private boolean waitLatch = true;

	@Autowired(required = false)
	private YarnContainer yarnContainer;

	private final StateWrapper stateWrapper = new StateWrapper();

	@Override
	public void run(String... args) throws Exception {
		if (yarnContainer != null) {
			launchContainer(yarnContainer, args);
		}
	}

	public void setWaitLatch(boolean waitLatch) {
		this.waitLatch = waitLatch;
	}

	protected void launchContainer(YarnContainer container, String[] parameters) {
		Properties properties = StringUtils.splitArrayElementsIntoProperties(parameters, "=");
		container.setParameters(properties != null ? properties : new Properties());
		container.setEnvironment(System.getenv());

		log.info("Running YarnContainer with parameters [" + StringUtils.arrayToCommaDelimitedString(parameters) + "]");

		// use latch if container wants to be long running
		if (container instanceof LongRunningYarnContainer && ((LongRunningYarnContainer)container).isWaitCompleteState()) {
			log.info("Container requested that we wait state, setting up latch");
			latch = new CountDownLatch(1);
			((LongRunningYarnContainer)container).addContainerStateListener(new ContainerStateListener() {
				@Override
				public void state(ContainerState state, Object exit) {
					if (log.isDebugEnabled()) {
						log.debug("Got state ContainerState=" + state + " and exit=" + exit);
					}
					stateWrapper.state = state;
					stateWrapper.exit = exit;
					latch.countDown();
				}
			});
		}

		// tell container to do its stuff
		container.run();

		if (waitLatch) {
			if (latch != null) {
				try {
					// TODO: should we use timeout?
					latch.await();
				} catch (InterruptedException e) {
					log.info("YarnContainer latch wait interrupted");
				}
			}

			log.info("YarnContainer complete");
			int exitCode = 0;
			if (stateWrapper.state != null) {
				if (stateWrapper.exit != null) {
					if (stateWrapper.exit instanceof String) {
						exitCode = exitCodeMapper.intValue((String)stateWrapper.exit);
					} else if (stateWrapper.exit instanceof Boolean) {
						exitCode = exitCodeMapper.intValue((Boolean)stateWrapper.exit);
					} else if (stateWrapper.exit instanceof Integer) {
						exitCode = (Integer) stateWrapper.exit;
					}
				}
			}
			log.info("Exiting with exitCode=" + exitCode);
			systemExiter.exit(exitCode);
		}
	}

	private static class StateWrapper {
		ContainerState state;
		Object exit;
	}

}
