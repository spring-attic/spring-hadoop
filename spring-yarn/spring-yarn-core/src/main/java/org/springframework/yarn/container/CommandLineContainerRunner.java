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
package org.springframework.yarn.container;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.StringUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.launch.AbstractCommandLineRunner;
import org.springframework.yarn.launch.ExitStatus;

/**
 * A simple container runner executing a bean
 * named "yarnContainer".
 *
 * @author Janne Valkealahti
 *
 */
public class CommandLineContainerRunner extends AbstractCommandLineRunner<YarnContainer> {

	private static final Log log = LogFactory.getLog(CommandLineContainerRunner.class);

	@Override
	protected ExitStatus handleBeanRun(YarnContainer bean, String[] parameters, Set<String> opts) {
		Properties properties = StringUtils.splitArrayElementsIntoProperties(parameters, "=");
		bean.setParameters(properties != null ? properties : new Properties());
		bean.setEnvironment(System.getenv());
		if(log.isDebugEnabled()) {
			log.debug("Starting YarnClient bean: " + StringUtils.arrayToCommaDelimitedString(parameters));
		}
		bean.run();
		if(log.isDebugEnabled()) {
			log.debug("YarnClient bean complete");
		}
		return ExitStatus.COMPLETED;
	}

	@Override
	protected String getDefaultBeanIdentifier() {
		return YarnSystemConstants.DEFAULT_ID_CONTAINER;
	}

	@Override
	protected List<String> getValidOpts() {
		return null;
	}

	public static void main(String[] args) {
		new CommandLineContainerRunner().doMain(args);
	}

}
