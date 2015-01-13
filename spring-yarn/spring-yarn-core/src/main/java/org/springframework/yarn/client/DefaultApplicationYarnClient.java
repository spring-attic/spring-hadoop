/*
 * Copyright 2014-2015 the original author or authors.
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.util.StringUtils;
import org.springframework.yarn.YarnSystemException;

/**
 * An implementation of {@link ApplicationYarnClient} verifying application
 * install and submit statuses.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultApplicationYarnClient extends CommandYarnClient implements ApplicationYarnClient {

	/**
	 * Instantiates a new default application yarn client.
	 *
	 * @param clientRmOperations the client rm operations
	 */
	public DefaultApplicationYarnClient(ClientRmOperations clientRmOperations) {
		super(clientRmOperations);
	}

	@Override
	public void installApplication(ApplicationDescriptor descriptor) {
		preInstallVerify(descriptor);
		installApplication();
		postInstallVerify(descriptor);
	}

	@Override
	public ApplicationId submitApplication(ApplicationDescriptor descriptor) {
		preSubmitVerify(descriptor);
		// override app name if it has been set is descriptor
		if (StringUtils.hasText(descriptor.getName())) {
			setAppName(descriptor.getName());
		}
		ApplicationId applicationId = submitApplication(false);
		postSubmitVerify(applicationId, descriptor);
		return applicationId;
	}

	/**
	 * Pre install verify.
	 *
	 * @param descriptor the application descriptor
	 */
	protected void preInstallVerify(ApplicationDescriptor descriptor) {
		try {
			Path path = new Path(descriptor.getDirectory());
			FileSystem fs = path.getFileSystem(getConfiguration());
			if (fs.exists(path)) {
				throw new IllegalArgumentException("Application directory " + descriptor.getDirectory() + " already exists");
			}
		} catch (Exception e) {
			throw new YarnSystemException("Error", e);
		}
	}

	/**
	 * Post install verify.
	 *
	 * @param descriptor the application descriptor
	 */
	protected void postInstallVerify(ApplicationDescriptor descriptor) {
	}

	/**
	 * Pre submit verify.
	 *
	 * @param descriptor the application descriptor
	 */
	protected void preSubmitVerify(ApplicationDescriptor descriptor) {
		try {
			Path path = new Path(descriptor.getDirectory());
			FileSystem fs = path.getFileSystem(getConfiguration());
			if (!fs.exists(path)) {
				throw new IllegalArgumentException("Application directory " + descriptor.getDirectory() + " doesn't exist");
			}
		} catch (Exception e) {
			throw new YarnSystemException("Error", e);
		}
	}

	/**
	 * Post submit verify.
	 *
	 * @param applicationId the application id
	 * @param descriptor the application descriptor
	 */
	protected void postSubmitVerify(ApplicationId applicationId, ApplicationDescriptor descriptor) {
	}

}
