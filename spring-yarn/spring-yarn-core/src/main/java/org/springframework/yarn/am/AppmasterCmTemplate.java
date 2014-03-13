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
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.yarn.rpc.YarnRpcAccessor;
import org.springframework.yarn.rpc.YarnRpcCallback;
import org.springframework.yarn.support.compat.NMTokenCacheCompat;

/**
 * Template implementation for {@link AppmasterCmOperations} wrapping
 * communication using {@link ContainerManagementProtocol}. Methods for this
 * template wraps possible exceptions into Spring Dao exception hierarchy.
 *
 * @author Janne Valkealahti
 *
 */
public class AppmasterCmTemplate extends YarnRpcAccessor<ContainerManagementProtocol> implements AppmasterCmOperations {

	/** Container we're working for */
	private final Container container;

	/**
	 * Instantiates a new AppmasterCmTemplate.
	 *
	 * @param config the hadoop configation
	 * @param container the {@link Container}
	 */
	public AppmasterCmTemplate(Configuration config, Container container) {
		super(ContainerManagementProtocol.class, config);
		this.container = container;
	}

	@Override
	public StartContainersResponse startContainers(final StartContainersRequest request) {
		return execute(new YarnRpcCallback<StartContainersResponse, ContainerManagementProtocol>() {
			@Override
			public StartContainersResponse doInYarn(ContainerManagementProtocol proxy) throws YarnException, IOException {
				return proxy.startContainers(request);
			}
		});
	}

	@Override
	public StopContainersResponse stopContainers() {
		return execute(new YarnRpcCallback<StopContainersResponse, ContainerManagementProtocol>() {
			@Override
			public StopContainersResponse doInYarn(ContainerManagementProtocol proxy) throws YarnException, IOException {
				StopContainersRequest request = Records.newRecord(StopContainersRequest.class);
				ArrayList<ContainerId> ids = new ArrayList<ContainerId>();
				ids.add(container.getId());
				request.setContainerIds(ids);
				return proxy.stopContainers(request);
			}
		});
	}

	@Override
	public ContainerStatus getContainerStatus() {
		return execute(new YarnRpcCallback<ContainerStatus, ContainerManagementProtocol>() {
			@Override
			public ContainerStatus doInYarn(ContainerManagementProtocol proxy) throws YarnException, IOException {
				GetContainerStatusesRequest request = Records.newRecord(GetContainerStatusesRequest.class);
				ArrayList<ContainerId> ids = new ArrayList<ContainerId>();
				ids.add(container.getId());
				request.setContainerIds(ids);
				return proxy.getContainerStatuses(request).getContainerStatuses().get(0);
			}
		});
	}

	@Override
	protected InetSocketAddress getRpcAddress(Configuration config) {
		String cmIpPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort();
		return NetUtils.createSocketAddr(cmIpPortStr);
	}

	@SuppressWarnings("static-access")
	@Override
	protected UserGroupInformation getUser() {
		InetSocketAddress rpcAddress = getRpcAddress(getConfiguration());
		Token token = NMTokenCacheCompat.getNMTokenCache().getNMToken(container.getNodeId().toString());

		// this is what node manager requires for auth
		UserGroupInformation user =
				UserGroupInformation.createRemoteUser(container.getId().getApplicationAttemptId().toString());
		org.apache.hadoop.security.token.Token<NMTokenIdentifier> nmToken =
				ConverterUtils.convertFromYarn(token, rpcAddress);
		user.addToken(nmToken);

		return user;
	}

}
