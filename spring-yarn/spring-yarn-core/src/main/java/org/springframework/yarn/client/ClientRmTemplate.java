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

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
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
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.yarn.rpc.YarnRpcAccessor;
import org.springframework.yarn.rpc.YarnRpcCallback;

/**
 * Template implementation for {@link ClientRmOperations} wrapping
 * communication using {@link ClientRMProtocol}. Methods for this
 * template wraps possible exceptions into Spring Dao exception hierarchy.
 *
 * @author Janne Valkealahti
 *
 */
public class ClientRmTemplate extends YarnRpcAccessor<ClientRMProtocol> implements ClientRmOperations {

	/**
	 * Constructs a {@link ClientRmTemplate} with a given yarn configuration.
	 *
	 * @param config the yarn configuration
	 */
	public ClientRmTemplate(Configuration config) {
		super(ClientRMProtocol.class, config);
	}

	@Override
	public List<ApplicationReport> listApplications() {
		return execute(new YarnRpcCallback<List<ApplicationReport>, ClientRMProtocol>() {
			@Override
			public List<ApplicationReport> doInYarn(ClientRMProtocol proxy) throws YarnRemoteException {
				GetAllApplicationsRequest request = Records.newRecord(GetAllApplicationsRequest.class);
				GetAllApplicationsResponse response = proxy.getAllApplications(request);
				return response.getApplicationList();
			}
		});
	}

	@Override
	public GetNewApplicationResponse getNewApplication() {
		return execute(new YarnRpcCallback<GetNewApplicationResponse, ClientRMProtocol>() {
			@Override
			public GetNewApplicationResponse doInYarn(ClientRMProtocol proxy) throws YarnRemoteException {
				GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
				return proxy.getNewApplication(request);
			}
		});
	}

	@Override
	public SubmitApplicationResponse submitApplication(final ApplicationSubmissionContext appSubContext) {
		return execute(new YarnRpcCallback<SubmitApplicationResponse, ClientRMProtocol>() {
			@Override
			public SubmitApplicationResponse doInYarn(ClientRMProtocol proxy) throws YarnRemoteException {
				SubmitApplicationRequest request = Records.newRecord(SubmitApplicationRequest.class);
				request.setApplicationSubmissionContext(appSubContext);
				return proxy.submitApplication(request);
			}
		});
	}

	@Override
	public KillApplicationResponse killApplication(final ApplicationId applicationId) {
		return execute(new YarnRpcCallback<KillApplicationResponse, ClientRMProtocol>() {
			@Override
			public KillApplicationResponse doInYarn(ClientRMProtocol proxy) throws YarnRemoteException {
				KillApplicationRequest request = Records.newRecord(KillApplicationRequest.class);
				request.setApplicationId(applicationId);
				return proxy.forceKillApplication(request);
			}
		});
	}

	@Override
	public DelegationToken getDelegationToken(final String renewer) {
		return execute(new YarnRpcCallback<DelegationToken, ClientRMProtocol>() {
			@Override
			public DelegationToken doInYarn(ClientRMProtocol proxy) throws YarnRemoteException {
				GetDelegationTokenRequest request = Records.newRecord(GetDelegationTokenRequest.class);
				request.setRenewer(renewer);
				return proxy.getDelegationToken(request).getRMDelegationToken();
			}
		});
	}
	
	@Override
	public ApplicationReport getApplicationReport(final ApplicationId applicationId) {
		return execute(new YarnRpcCallback<ApplicationReport, ClientRMProtocol>() {
			@Override
			public ApplicationReport doInYarn(ClientRMProtocol proxy) throws YarnRemoteException {
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
