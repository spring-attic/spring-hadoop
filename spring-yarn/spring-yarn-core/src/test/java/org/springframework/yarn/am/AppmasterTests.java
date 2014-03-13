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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class AppmasterTests {

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testAppmaster() throws Exception {
		assertTrue(ctx.containsBean("yarnAppmaster"));

		YarnAppmaster master = (YarnAppmaster) ctx.getBean("yarnAppmaster");
		assertNotNull(master);

        AppmasterService service = ((AbstractAppmaster)master).getAppmasterService();
        assertThat(service, notNullValue());

        ReflectionTestUtils.invokeMethod(master, "getAppmasterService", new Object[0]);
	}

	public static class TestAppmaster extends StaticAppmaster {
		@Override
		protected void onInit() throws Exception {
			Map<String, String> environment = new HashMap<String, String>();
			environment.put("CONTAINER_ID", "container_1360089121174_0011_01_000001");
			setEnvironment(environment);
			super.onInit();
		}
	}

	public static class StubAppmasterService implements AppmasterService {
		@Override
		public int getPort() {
			return 0;
		}
		@Override
		public String getHost() {
			return null;
		}
		@Override
		public boolean hasPort() {
			return true;
		}
	}


}
