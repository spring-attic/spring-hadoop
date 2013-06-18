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
package org.springframework.yarn.integration.ip.mind;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.am.AppmasterService;
import org.springframework.yarn.am.AppmasterServiceClient;

/**
 * Integration tests around namespace configuration.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class MindIntegrationDefaultTests {

	@Autowired
	ApplicationContext ctx;

	@Autowired
	AppmasterService appmasterService;

	@Autowired
	AppmasterServiceClient appmasterServiceClient;

	@Test
	public void testServiceInterfaces() throws Exception {
		assertNotNull(appmasterService);
		assertNotNull(appmasterServiceClient);

        assumeTrue(appmasterService.getPort() > 0); //TODO: if we didn't get a good port - skip test for now
		SimpleTestRequest request = new SimpleTestRequest();
		SimpleTestResponse response = (SimpleTestResponse) ((MindAppmasterServiceClient)appmasterServiceClient).doMindRequest(request);
		assertNotNull(response);
		assertThat(response.stringField, is("echo:stringFieldValue"));
	}

}
