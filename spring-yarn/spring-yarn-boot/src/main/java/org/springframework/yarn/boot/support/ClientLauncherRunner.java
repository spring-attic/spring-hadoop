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
package org.springframework.yarn.boot.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.util.StringUtils;
import org.springframework.yarn.client.YarnClient;

/**
 * A {@link CommandLineRunner} handling a default action for {@link YarnClient}.
 *
 * @author Janne Valkealahti
 *
 */
public class ClientLauncherRunner implements CommandLineRunner {

	private static final Log log = LogFactory.getLog(ClientLauncherRunner.class);

	public static final String ACTION_SUBMIT = "submit";

	@Autowired(required=false)
	private YarnClient yarnClient;

	private String action;

	/**
	 * Instantiates a new client launcher runner.
	 */
	public ClientLauncherRunner() {
	}

	/**
	 * Instantiates a new client launcher runner.
	 *
	 * @param action the action
	 */
	public ClientLauncherRunner(String action) {
		this.action = action;
	}

	@Override
	public void run(String... args) throws Exception {
		if (!StringUtils.hasText(action)) {
			// no action, silently bail out
			return;
		} else if (yarnClient == null) {
			log.warn("We have action=" + action + " but no yarnClient");
			return;
		}
		log.info("Running action=" + action);
		if (ACTION_SUBMIT.equals(action)) {
			log.info("Submitting application");
			yarnClient.submitApplication();
		} else {
			log.warn("Unable to do startup with unknown action=" + action);
		}
	}

	/**
	 * Sets the action use by this launcher.
	 *
	 * @param action the new action
	 */
	public void setAction(String action) {
		this.action = action;
	}

}
