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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.yarn.rpc.YarnRpcAccessor;
import org.springframework.yarn.rpc.YarnRpcCallback;

/**
 * Template implementation for {@link AppmasterRmOperations} wrapping
 * communication using {@link AMRMProtocol}. Methods for this
 * template wraps possible exceptions into Spring Dao exception hierarchy.
 *
 * @author Janne Valkealahti
 *
 */
public class AppmasterRmTemplate extends YarnRpcAccessor<AMRMProtocol> implements AppmasterRmOperations {

	private static final Log log = LogFactory.getLog(AppmasterCmTemplate.class);

	public AppmasterRmTemplate(Configuration config) {
		super(AMRMProtocol.class, config);
	}

	@Override
	public RegisterApplicationMasterResponse registerApplicationMaster(final ApplicationAttemptId appAttemptId, final String host, final Integer rpcPort, final String trackUrl) {
		return execute(new YarnRpcCallback<RegisterApplicationMasterResponse, AMRMProtocol>() {
			@Override
			public RegisterApplicationMasterResponse doInYarn(AMRMProtocol proxy) throws YarnRemoteException {
				RegisterApplicationMasterRequest appMasterRequest = Records.newRecord(RegisterApplicationMasterRequest.class);
				appMasterRequest.setApplicationAttemptId(appAttemptId);
				appMasterRequest.setHost(host != null ? host : "");
				appMasterRequest.setRpcPort(rpcPort != null ? rpcPort : 0);
				appMasterRequest.setTrackingUrl(trackUrl != null ? trackUrl : "");
				return proxy.registerApplicationMaster(appMasterRequest);
			}
		});
	}

	@Override
	public AllocateResponse allocate(final AllocateRequest request) {
		return execute(new YarnRpcCallback<AllocateResponse, AMRMProtocol>() {
			@Override
			public AllocateResponse doInYarn(AMRMProtocol proxy) throws YarnRemoteException {
				return proxy.allocate(request);
			}
		});
	}

	@Override
	public FinishApplicationMasterResponse finish(final FinishApplicationMasterRequest request) {
		return execute(new YarnRpcCallback<FinishApplicationMasterResponse, AMRMProtocol>() {
			@Override
			public FinishApplicationMasterResponse doInYarn(AMRMProtocol proxy) throws YarnRemoteException {
				return proxy.finishApplicationMaster(request);
			}
		});
	}

	@Override
	protected InetSocketAddress getRpcAddress(Configuration config) {
		InetSocketAddress addr = config.getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS, YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

		UserGroupInformation currentUser;
		try {
			currentUser = UserGroupInformation.getCurrentUser();
		} catch (IOException e) {
			throw new YarnException(e);
		}

		if (UserGroupInformation.isSecurityEnabled()) {
			String tokenURLEncodedStr = System.getenv().get(
				ApplicationConstants.APPLICATION_MASTER_TOKEN_ENV_NAME);
			Token<? extends TokenIdentifier> token = new Token<TokenIdentifier>();

			try {
			token.decodeFromUrlString(tokenURLEncodedStr);
			} catch (IOException e) {
			throw new YarnException(e);
			}

			SecurityUtil.setTokenService(token, addr);
			if (log.isDebugEnabled()) {
				log.debug("AppMasterToken is " + token);
			}
			currentUser.addToken(token);
		}
		return addr;
	}

}
