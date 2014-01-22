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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.yarn.rpc.YarnRpcAccessor;
import org.springframework.yarn.rpc.YarnRpcCallback;

/**
 * Template implementation for {@link ClientRmOperations} wrapping
 * communication using {@link ApplicationClientProtocol}. Methods for this
 * template wraps possible exceptions into Spring Dao exception hierarchy.
 *
 * @author Janne Valkealahti
 *
 */
public class ClientRmTemplate extends YarnRpcAccessor<ApplicationClientProtocol> implements ClientRmOperations {

	/**
	 * Constructs a {@link ClientRmTemplate} with a given yarn configuration.
	 *
	 * @param config the yarn configuration
	 */
	public ClientRmTemplate(Configuration config) {
		super(ApplicationClientProtocol.class, config);
	}

	@Override
	public List<ApplicationReport> listApplications(final EnumSet<YarnApplicationState> states, final Set<String> types) {
		return execute(new YarnRpcCallback<List<ApplicationReport>, ApplicationClientProtocol>() {
			@Override
			public List<ApplicationReport> doInYarn(ApplicationClientProtocol proxy) throws YarnException, IOException {
				GetApplicationsRequest request = Records.newRecord(GetApplicationsRequest.class);
				request.setApplicationStates(states);
				request.setApplicationTypes(types);
				GetApplicationsResponse response = proxy.getApplications(request);
				return response.getApplicationList();
			}
		});
	}

	@Override
	public List<ApplicationReport> listApplications() {
		return execute(new YarnRpcCallback<List<ApplicationReport>, ApplicationClientProtocol>() {
			@Override
			public List<ApplicationReport> doInYarn(ApplicationClientProtocol proxy) throws YarnException, IOException {
				GetApplicationsRequest request = Records.newRecord(GetApplicationsRequest.class);
				GetApplicationsResponse response = proxy.getApplications(request);
				return response.getApplicationList();
			}
		});
	}

	@Override
	public GetNewApplicationResponse getNewApplication() {
		return execute(new YarnRpcCallback<GetNewApplicationResponse, ApplicationClientProtocol>() {
			@Override
			public GetNewApplicationResponse doInYarn(ApplicationClientProtocol proxy) throws YarnException, IOException {
				GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
				return proxy.getNewApplication(request);
			}
		});
	}

	@Override
	public SubmitApplicationResponse submitApplication(final ApplicationSubmissionContext appSubContext) {
		return execute(new YarnRpcCallback<SubmitApplicationResponse, ApplicationClientProtocol>() {
			@Override
			public SubmitApplicationResponse doInYarn(ApplicationClientProtocol proxy) throws YarnException, IOException {
				SubmitApplicationRequest request = Records.newRecord(SubmitApplicationRequest.class);
				request.setApplicationSubmissionContext(appSubContext);
				return proxy.submitApplication(request);
			}
		});
	}

	@Override
	public KillApplicationResponse killApplication(final ApplicationId applicationId) {
		return execute(new YarnRpcCallback<KillApplicationResponse, ApplicationClientProtocol>() {
			@Override
			public KillApplicationResponse doInYarn(ApplicationClientProtocol proxy) throws YarnException, IOException {
				KillApplicationRequest request = Records.newRecord(KillApplicationRequest.class);
				request.setApplicationId(applicationId);
				return proxy.forceKillApplication(request);
			}
		});
	}

	@Override
	public Token getDelegationToken(final String renewer) {
		return execute(new YarnRpcCallback<Token, ApplicationClientProtocol>() {
			@Override
			public Token doInYarn(ApplicationClientProtocol proxy) throws YarnException, IOException {
				GetDelegationTokenRequest request = Records.newRecord(GetDelegationTokenRequest.class);
				request.setRenewer(renewer);
				return proxy.getDelegationToken(request).getRMDelegationToken();
			}
		});
	}

	@Override
	public ApplicationReport getApplicationReport(final ApplicationId applicationId) {
		return execute(new YarnRpcCallback<ApplicationReport, ApplicationClientProtocol>() {
			@Override
			public ApplicationReport doInYarn(ApplicationClientProtocol proxy) throws YarnException, IOException {
				GetApplicationReportRequest request = Records.newRecord(GetApplicationReportRequest.class);
				request.setApplicationId(applicationId);
				return proxy.getApplicationReport(request).getApplicationReport();
			}
		});
	}

	@Override
	protected InetSocketAddress getRpcAddress(Configuration config) {
		return config.getSocketAddr(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS,
				YarnConfiguration.DEFAULT_RM_PORT);
	}

}
