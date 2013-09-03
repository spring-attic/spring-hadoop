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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
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
public class AppmasterCmTemplate extends YarnRpcAccessor<ContainerManagementProtocol> implements AppmasterCmOperations {

	private final static Log log = LogFactory.getLog(AppmasterCmTemplate.class);



	/** Container we're working for */
	private final Container container;

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
				// TODO: 210 now using list
				return proxy.stopContainers(request);
//				request.setContainerId(container.getId());
//				return proxy.stopContainer(request);
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
				// TODO: 210 now using list
				return proxy.getContainerStatuses(request).getContainerStatuses().get(0);
//				request.setContainerId(container.getId());
//				return proxy.getContainerStatuses(request).getStatus();
			}
		});
	}

	@Override
	protected InetSocketAddress getRpcAddress(Configuration config) {
		String cmIpPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort();
		return NetUtils.createSocketAddr(cmIpPortStr);
	}

	// TODO: 210 fix user auth
	@Override
	protected UserGroupInformation getUser() {
		UserGroupInformation user = null;
//		try {
//			user = UserGroupInformation.getCurrentUser();
			InetSocketAddress rpcAddress = getRpcAddress(getConfiguration());

			Token token = NMTokenCache.getNMToken(container.getNodeId().toString());
			log.info("XXXX: from cache token="+token);
			user = UserGroupInformation.createRemoteUser(container.getId().getApplicationAttemptId().toString());

			log.info("XXXX: user=" + user);

		org.apache.hadoop.security.token.Token<NMTokenIdentifier> nmToken = ConverterUtils.convertFromYarn(token,
				rpcAddress);
		log.info("XXXX: from nmToken="+nmToken);
		user.addToken(nmToken);

//			if (UserGroupInformation.isSecurityEnabled()) {
//				ContainerToken containerToken = container.getContainerToken();
//				Token<ContainerTokenIdentifier> token = null;
//				if (containerToken instanceof DelegationToken) {
//					token = convertFromProtoFormat((DelegationToken) container.getContainerToken(),
//							getRpcAddress(getConfiguration()));
//				}
//				// remote user needs to be a container id
//				user = UserGroupInformation.createRemoteUser(container.getId().toString());
//				user.addToken(token);
//			}
//		} catch (IOException e) {
//		}
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
//	private static <T extends TokenIdentifier> Token<T> convertFromProtoFormat(DelegationToken protoToken,
//			InetSocketAddress serviceAddr) {
//		// TODO: remove this method when api's are compatible
//		Token<T> token = new Token<T>(protoToken.getIdentifier().array(), protoToken.getPassword().array(),
//				new Text(protoToken.getKind()), new Text(protoToken.getService()));
//		if (serviceAddr != null) {
//			SecurityUtil.setTokenService(token, serviceAddr);
//		}
//		return token;
//	}

}
