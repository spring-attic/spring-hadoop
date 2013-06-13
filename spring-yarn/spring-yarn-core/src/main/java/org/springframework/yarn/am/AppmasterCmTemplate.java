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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.yarn.rpc.YarnRpcAccessor;
import org.springframework.yarn.rpc.YarnRpcCallback;

/**
 * Template implementation for {@link AppmasterCmOperations} wrapping
 * communication using {@link ContainerManager}. Methods for this
 * template wraps possible exceptions into Spring Dao exception hierarchy.
 *
 * @author Janne Valkealahti
 *
 */
public class AppmasterCmTemplate extends YarnRpcAccessor<ContainerManager> implements AppmasterCmOperations {

	/** Container we're working for */
	private final Container container;

	public AppmasterCmTemplate(Configuration config, Container container) {
		super(ContainerManager.class, config);
		this.container = container;
	}

	@Override
	public StartContainerResponse startContainer(final StartContainerRequest request) {
		return execute(new YarnRpcCallback<StartContainerResponse, ContainerManager>() {
			@Override
			public StartContainerResponse doInYarn(ContainerManager proxy) throws YarnRemoteException {
				return proxy.startContainer(request);
			}
		});
	}

	@Override
	public StopContainerResponse stopContainer() {
		return execute(new YarnRpcCallback<StopContainerResponse, ContainerManager>() {
			@Override
			public StopContainerResponse doInYarn(ContainerManager proxy) throws YarnRemoteException {
				StopContainerRequest request = Records.newRecord(StopContainerRequest.class);
				request.setContainerId(container.getId());
				return proxy.stopContainer(request);
			}
		});
	}

	@Override
	public ContainerStatus getContainerStatus() {
		return execute(new YarnRpcCallback<ContainerStatus, ContainerManager>() {
			@Override
			public ContainerStatus doInYarn(ContainerManager proxy) throws YarnRemoteException {
				GetContainerStatusRequest request = Records.newRecord(GetContainerStatusRequest.class);
				request.setContainerId(container.getId());
				return proxy.getContainerStatus(request).getStatus();
			}
		});
	}

	@Override
	protected InetSocketAddress getRpcAddress(Configuration config) {
		String cmIpPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort();
		return NetUtils.createSocketAddr(cmIpPortStr);
	}

	@Override
	protected UserGroupInformation getUser() {
		UserGroupInformation user = null;
		try {
			user = UserGroupInformation.getCurrentUser();
			if (UserGroupInformation.isSecurityEnabled()) {
				ContainerToken containerToken = container.getContainerToken();
				Token<ContainerTokenIdentifier> token = null;
				if (containerToken instanceof DelegationToken) {
					token = convertFromProtoFormat((DelegationToken) container.getContainerToken(),
							getRpcAddress(getConfiguration()));
				}
				// remote user needs to be a container id
				user = UserGroupInformation.createRemoteUser(container.getId().toString());
				user.addToken(token);
			}
		} catch (IOException e) {
		}
		return user;
	}

	/**
	 * Convert token identifier from a proto format.
	 * <p>
	 * This function is a copy for way it was pre hadoop-2.0.3. Helps
	 * to work with api changes.
	 *
	 * @param <T> the generic type
	 * @param protoToken the proto token
	 * @param serviceAddr the service addr
	 * @return the token identifier
	 */
	private static <T extends TokenIdentifier> Token<T> convertFromProtoFormat(DelegationToken protoToken,
			InetSocketAddress serviceAddr) {
		// TODO: remove this method when api's are compatible
		Token<T> token = new Token<T>(protoToken.getIdentifier().array(), protoToken.getPassword().array(),
				new Text(protoToken.getKind()), new Text(protoToken.getService()));
		if (serviceAddr != null) {
			SecurityUtil.setTokenService(token, serviceAddr);
		}
		return token;
	}

}
