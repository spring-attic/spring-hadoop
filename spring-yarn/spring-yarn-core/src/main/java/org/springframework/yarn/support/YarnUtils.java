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
package org.springframework.yarn.support;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;
import org.springframework.yarn.YarnSystemException;

/**
 * Different utilities.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnUtils {

	/**
	 * Converts {@link YarnRemoteException} to a Spring dao exception.
	 *
	 * @param e the {@link YarnRemoteException}
	 * @return a wrapped native exception into {@link DataAccessException}
	 */
	public static DataAccessException convertYarnAccessException(YarnRemoteException e) {
		return new YarnSystemException(e);
	}

	/**
	 * Converts {@link RemoteException} to a Spring dao exception.
	 *
	 * @param e the {@link RemoteException}
	 * @return a wrapped native exception into {@link DataAccessException}
	 */
	public static DataAccessException convertYarnAccessException(RemoteException e) {
		return new YarnSystemException(e);
	}

	/**
	 * Converts {@link YarnException} to a Spring dao exception.
	 *
	 * @param e the {@link YarnException}
	 * @return a wrapped native exception into {@link DataAccessException}
	 */
	public static DataAccessException convertYarnAccessException(YarnException e) {
		return new YarnSystemException(e);
	}

	/**
	 * Gets {@link ApplicationAttemptId} from environment variables.
	 *
	 * @param environment Map of environment variables
	 * @return the {@link ApplicationAttemptId}
	 */
	public static ApplicationAttemptId getApplicationAttemptId(Map<String, String> environment) {
		String amContainerId = environment.get(ApplicationConstants.AM_CONTAINER_ID_ENV);
		Assert.notNull(amContainerId, "AM_CONTAINER_ID env variable has to exist to build appAttemptId");
		ContainerId containerId = ConverterUtils.toContainerId(amContainerId);
		return containerId.getApplicationAttemptId();
	}

	/**
	 * Gets the principal.
	 *
	 * @param conf the conf
	 * @return the principal
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static String getPrincipal(Configuration conf) throws IOException {
		String masterHostname = getAddress(conf).getHostName();
		return SecurityUtil.getServerPrincipal(getUserName(conf), masterHostname);
	}

	/**
	 * Gets the user name.
	 *
	 * @param conf the Yarn configuration
	 * @return the user name
	 */
	public static String getUserName(Configuration conf) {
		return conf.get(YarnConfiguration.RM_PRINCIPAL);
	}

	/**
	 * Gets the address.
	 *
	 * @param conf the Yarn configuration
	 * @return the address
	 */
	public static InetSocketAddress getAddress(Configuration conf) {
		return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS,
				YarnConfiguration.DEFAULT_RM_PORT);
	}

}
