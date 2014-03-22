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

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;

import javax.annotation.Resource;

import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.rpc.YarnRpcCallback;
import org.springframework.yarn.test.context.MiniYarnCluster;
import org.springframework.yarn.test.context.YarnDelegatingSmartContextLoader;

/**
 * Tests for {@link ClientRmTemplate}.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster
@DirtiesContext(classMode=ClassMode.AFTER_CLASS)
public class ClientRmTemplateTests {

	@Resource(name = "yarnClientRmTemplate")
	private ClientRmTemplate template;

	@Test
	public void testGetNewApplicationResponse() {
		assertNotNull(template);

		GetNewApplicationResponse response = template.getNewApplication();
		assertNotNull(response);
	}

	@Test
	public void testListApplications() {
		List<ApplicationReport> applications = template.listApplications();
		assertNotNull(applications);
	}

	@Test
	public void testExecuteCallback() {
		List<ApplicationReport> applications = template.execute(new YarnRpcCallback<List<ApplicationReport>, ApplicationClientProtocol>() {
			@Override
			public List<ApplicationReport> doInYarn(ApplicationClientProtocol proxy) throws YarnException, IOException {
				GetApplicationsRequest request = Records.newRecord(GetApplicationsRequest.class);
				GetApplicationsResponse response = proxy.getApplications(request);
				return response.getApplicationList();
			}
		});
		assertNotNull(applications);
	}

}
