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
package org.springframework.yarn.test.junit;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.test.context.MiniYarnCluster;
import org.springframework.yarn.test.context.YarnDelegatingSmartContextLoader;

/**
 * Tests for {@link AbstractYarnClusterTests}.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster
@DirtiesContext(classMode=ClassMode.AFTER_CLASS)
public class ClusterBaseTestClassTests extends AbstractYarnClusterTests {

	@Test
	public void testBaseFunctionality() {
		assertNotNull(getApplicationContext());
		assertNotNull(getConfiguration());
		assertNotNull(getYarnCluster());
	}

	@Override
	@Autowired(required=false)
	public void setYarnClient(YarnClient yarnClient) {
		// context doesn't have client so override
		// it here for test not to fail
		super.setYarnClient(yarnClient);
	}

}
