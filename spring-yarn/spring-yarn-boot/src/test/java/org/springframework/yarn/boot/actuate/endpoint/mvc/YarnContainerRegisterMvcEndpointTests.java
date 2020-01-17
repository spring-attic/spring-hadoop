/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.yarn.boot.actuate.endpoint.mvc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointAutoConfiguration;
import org.springframework.boot.actuate.endpoint.invoke.ParameterValueMapper;
import org.springframework.boot.actuate.endpoint.invoke.convert.ConversionServiceParameterValueMapper;
import org.springframework.boot.autoconfigure.hateoas.HypermediaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.boot.actuate.endpoint.YarnContainerRegisterEndpoint;
import org.springframework.yarn.event.AbstractYarnEvent;
import org.springframework.yarn.event.ContainerRegisterEvent;
import org.springframework.yarn.event.DefaultYarnEventPublisher;
import org.springframework.yarn.event.YarnEventPublisher;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = { YarnContainerRegisterMvcEndpointTests.TestConfiguration.class })
@WebAppConfiguration
@DirtiesContext(classMode=ClassMode.AFTER_EACH_TEST_METHOD)
public class YarnContainerRegisterMvcEndpointTests {

	private final static String BASE = "/" + YarnContainerRegisterEndpoint.ENDPOINT_ID;

	@Autowired
	private WebApplicationContext context;

	private MockMvc mvc;

	private TestYarnAppmaster appmaster;

	@Before
	public void setUp() {
		mvc = MockMvcBuilders.webAppContextSetup(context).build();
		appmaster = (TestYarnAppmaster) context.getBean(YarnAppmaster.class);
	}

	@Test
	public void testHomeEmpty() throws Exception {
		mvc.
			perform(get(BASE)).
			andExpect(status().isOk()).
			andExpect(content().string(not(emptyString()))).
			andExpect(jsonPath("$.*", hasSize(1))).
			andExpect(jsonPath("$.message", containsString("Use POST")));
	}

	@Test
	public void testHomeReport() throws Exception {
		String content = "{\"containerId\":\"container_1360089121174_0011_01_000001\",\"trackUrl\":\"bar\"}";
		mvc.
			perform(post(BASE).content(content).contentType(MediaType.APPLICATION_JSON)).
			andExpect(status().is(202));

		assertThat(appmaster.events.size(), is(1));
	}

	@Import({ WebEndpointAutoConfiguration.class,
			HypermediaAutoConfiguration.class })
	@EnableWebMvc
	@Configuration
	public static class TestConfiguration {

  	    @Bean
		public YarnContainerRegisterEndpoint endpoint() {
			return new YarnContainerRegisterEndpoint();
		}

		@Bean
		public YarnContainerRegisterMvcEndpoint mvcEndpoint() {
			return new YarnContainerRegisterMvcEndpoint(endpoint());
		}

		@Bean
		public YarnAppmaster appMaster() {
			TestYarnAppmaster appmaster = new TestYarnAppmaster();
			return appmaster;
		}

		@Bean
		public YarnEventPublisher yarnEventPublisher() {
			return new DefaultYarnEventPublisher();
		}
		
		@Bean
		public ParameterValueMapper conversionServiceParameterValueMapper() {
		  return new ConversionServiceParameterValueMapper();
		}

	}

	protected static class TestYarnAppmaster extends StaticEventingAppmaster {

		List<ContainerRegisterEvent> events = new ArrayList<ContainerRegisterEvent>();

		@Override
		public void onApplicationEvent(AbstractYarnEvent event) {

			if (event instanceof ContainerRegisterEvent) {
				events.add((ContainerRegisterEvent) event);
			}

			super.onApplicationEvent(event);
		}
		@Override
		protected void onInit() throws Exception {
			setConfiguration(new org.apache.hadoop.conf.Configuration());
			super.onInit();
		}
		@Override
		protected void doStart() {
		}
		@Override
		protected void doStop() {
		}
	}

}
