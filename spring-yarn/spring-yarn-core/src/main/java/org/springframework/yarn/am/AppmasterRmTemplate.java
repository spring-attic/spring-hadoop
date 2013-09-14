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
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.yarn.rpc.YarnRpcAccessor;
import org.springframework.yarn.rpc.YarnRpcCallback;

/**
 * Template implementation for {@link AppmasterRmOperations} wrapping
 * communication using {@link ApplicationMasterProtocol}. Methods for this
 * template wraps possible exceptions into Spring Dao exception hierarchy.
 *
 * @author Janne Valkealahti
 *
 */
public class AppmasterRmTemplate extends YarnRpcAccessor<ApplicationMasterProtocol>
		implements AppmasterRmOperations {

	private static final Log log = LogFactory.getLog(AppmasterCmTemplate.class);

	/**
	 * Instantiates a new AppmasterRmTemplate.
	 *
	 * @param config the hadoop configuration
	 */
	public AppmasterRmTemplate(Configuration config) {
		super(ApplicationMasterProtocol.class, config);
	}

	@Override
	public RegisterApplicationMasterResponse registerApplicationMaster(final String host,
			final Integer rpcPort, final String trackUrl) {
		return execute(new YarnRpcCallback<RegisterApplicationMasterResponse, ApplicationMasterProtocol>() {
			@Override
			public RegisterApplicationMasterResponse doInYarn(ApplicationMasterProtocol proxy) throws YarnException,
					IOException {
				RegisterApplicationMasterRequest appMasterRequest = Records
						.newRecord(RegisterApplicationMasterRequest.class);
				appMasterRequest.setHost(host != null ? host : "");
				appMasterRequest.setRpcPort(rpcPort != null ? rpcPort : 0);
				appMasterRequest.setTrackingUrl(trackUrl != null ? trackUrl : "");
				return proxy.registerApplicationMaster(appMasterRequest);
			}
		});
	}

	@Override
	public AllocateResponse allocate(final AllocateRequest request) {
		return execute(new YarnRpcCallback<AllocateResponse, ApplicationMasterProtocol>() {
			@Override
			public AllocateResponse doInYarn(ApplicationMasterProtocol proxy) throws YarnException, IOException {
				return proxy.allocate(request);
			}
		});
	}

	@Override
	public FinishApplicationMasterResponse finish(final FinishApplicationMasterRequest request) {
		return execute(new YarnRpcCallback<FinishApplicationMasterResponse, ApplicationMasterProtocol>() {
			@Override
			public FinishApplicationMasterResponse doInYarn(ApplicationMasterProtocol proxy) throws YarnException,
					IOException {
				return proxy.finishApplicationMaster(request);
			}
		});
	}

	@Override
	protected InetSocketAddress getRpcAddress(Configuration config) {
		InetSocketAddress addr = config.getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS, YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
		try {
			setupTokens(addr);
		} catch (IOException e) {
			log.error("Error setting up tokens", e);
		}
		return addr;
	}

	private static void setupTokens(InetSocketAddress resourceManagerAddress) throws IOException {
		// It is assumed for now that the only AMRMToken in AM's UGI is for this
		// cluster/RM. TODO: Fix later when we have some kind of cluster-ID as
		// default service-address, see YARN-986.
		for (Token<? extends TokenIdentifier> token : UserGroupInformation.getCurrentUser().getTokens()) {
			if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
				// This token needs to be directly provided to the AMs, so set
				// the appropriate service-name. We'll need more infrastructure when
				// we need to set it in HA case.
				SecurityUtil.setTokenService(token, resourceManagerAddress);
			}
		}
	}

}
