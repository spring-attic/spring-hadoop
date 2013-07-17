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
package org.springframework.yarn.client;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.yarn.YarnSystemConstants;

/**
 * Default Yarn client utilising functionality in {@link AbstractYarnClient}.
 *
 * @author Janne Valkealahti
 *
 */
public class CommandYarnClient extends AbstractYarnClient {
	
	private final static Log log = LogFactory.getLog(CommandYarnClient.class);

	/**
	 * Constructs a default client with a given template.
	 *
	 * @param clientRmOperations the client to resource manager template
	 */
	public CommandYarnClient(ClientRmOperations clientRmOperations) {
		super(clientRmOperations);
	}
	
	@Override
	public Map<String, String> getEnvironment() {
		// For the environment we set additional env variables
		// from configuration. This is most useful for unit
		// testing where resource manager and hdfs are created 
		// dynamically within mini clusters.
		Map<String, String> env = super.getEnvironment();
		env.put(YarnSystemConstants.RM_ADDRESS, getConfiguration().get(YarnConfiguration.RM_ADDRESS));		
		env.put(YarnSystemConstants.FS_ADDRESS, getConfiguration().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
		env.put(YarnSystemConstants.SCHEDULER_ADDRESS, getConfiguration().get(YarnConfiguration.RM_SCHEDULER_ADDRESS));
		if (log.isDebugEnabled()) {
			log.debug("Setting additional env variables " +
					YarnSystemConstants.RM_ADDRESS + "=" + env.get(YarnSystemConstants.RM_ADDRESS) +
					YarnSystemConstants.FS_ADDRESS + "=" + env.get(YarnSystemConstants.FS_ADDRESS) +
					YarnSystemConstants.SCHEDULER_ADDRESS + "=" + env.get(YarnSystemConstants.SCHEDULER_ADDRESS));
		}
		return env;
	}

}
