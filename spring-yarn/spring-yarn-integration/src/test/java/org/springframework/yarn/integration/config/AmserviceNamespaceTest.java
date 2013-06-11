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
package org.springframework.yarn.integration.config;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.integration.IntegrationAppmasterServiceFactoryBean;
import org.springframework.yarn.integration.TestUtils;
import org.springframework.yarn.integration.ip.mind.TestService;

/**
 * Testing 'int:amservice' namespace.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("amservice-ns.xml")
public class AmserviceNamespaceTest {

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testDefaults() throws Exception {
		assertTrue(ctx.containsBean("yarnAmservice"));
		IntegrationAppmasterServiceFactoryBean fb =
				(IntegrationAppmasterServiceFactoryBean) ctx.getBean("&yarnAmservice");
		assertNotNull(fb);

		Class<?> clazz = TestUtils.readField("serviceImpl", fb);
		assertThat(clazz.getCanonicalName(), is("org.springframework.yarn.integration.ip.mind.TestService"));
		assertThat(TestUtils.readField("messageChannel", fb), instanceOf(DirectChannel.class));
		assertThat(TestUtils.readField("appmasterService", fb), instanceOf(TestService.class));

		TestService service = (TestService) ctx.getBean("yarnAmservice");
		assertNotNull(service);
		assertThat(TestUtils.readField("messageChannel", service), instanceOf(DirectChannel.class));

		testSocketSupport(service, fb);
	}

	@Test
	public void testWithChannelAndSocketSupport() throws Exception {
		assertTrue(ctx.containsBean("yarnAmservice2"));
		IntegrationAppmasterServiceFactoryBean fb =
				(IntegrationAppmasterServiceFactoryBean) ctx.getBean("&yarnAmservice2");
		assertNotNull(fb);

		Class<?> clazz = TestUtils.readField("serviceImpl", fb);
		assertThat(clazz.getCanonicalName(), is("org.springframework.yarn.integration.ip.mind.TestService"));
		assertThat(TestUtils.readField("messageChannel", fb), instanceOf(DirectChannel.class));
		assertThat(TestUtils.readField("appmasterService", fb), instanceOf(TestService.class));

		TestService service = (TestService) ctx.getBean("yarnAmservice2");
		assertNotNull(service);
		assertThat(TestUtils.readField("messageChannel", service), instanceOf(DirectChannel.class));

		testSocketSupport(service, fb);
	}

	@Test
	public void testWithReference() throws Exception {
		assertTrue(ctx.containsBean("yarnAmservice3"));
		IntegrationAppmasterServiceFactoryBean fb =
				(IntegrationAppmasterServiceFactoryBean) ctx.getBean("&yarnAmservice3");
		assertNotNull(fb);

		Class<?> clazz = TestUtils.readField("serviceImpl", fb);
		assertThat(clazz, nullValue());
		assertThat(TestUtils.readField("serviceRef", fb), instanceOf(TestService.class));
		assertThat(TestUtils.readField("messageChannel", fb), instanceOf(DirectChannel.class));
		assertThat(TestUtils.readField("appmasterService", fb), instanceOf(TestService.class));

		TestService service = (TestService) ctx.getBean("yarnAmservice3");
		assertNotNull(service);
		assertThat(TestUtils.readField("messageChannel", service), instanceOf(DirectChannel.class));

		testSocketSupport(service, fb);
	}

	private void testSocketSupport(Object bean, Object factory) throws Exception {
		assertThat(TestUtils.readField("socketSupport", factory), notNullValue());
		assertThat(TestUtils.readField("socketSupport", bean), notNullValue());
	}

}
