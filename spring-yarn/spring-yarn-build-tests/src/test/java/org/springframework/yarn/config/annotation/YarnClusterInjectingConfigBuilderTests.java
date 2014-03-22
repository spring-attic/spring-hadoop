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
package org.springframework.yarn.config.annotation;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.builders.YarnConfigConfigurer;
import org.springframework.yarn.test.context.MiniYarnCluster;
import org.springframework.yarn.test.context.YarnDelegatingSmartContextLoader;

/**
 * Testing configuration injecting together with minicluster,
 * configuration builder and autowiring.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster(configName="miniYarnConfiguration")
@DirtiesContext(classMode=ClassMode.AFTER_CLASS)
public class YarnClusterInjectingConfigBuilderTests {

	@Autowired
	private ApplicationContext ctx;

	@Resource(name = "yarnConfiguration")
	private Configuration configuration;

	@Test
	public void testLoaderAndConfig() {
		assertThat(ctx, notNullValue());
		assertThat(ctx.containsBean("yarnCluster"), is(true));
		assertThat(ctx.containsBean("yarnConfiguration"), is(true));
		Configuration config = (Configuration) ctx.getBean("yarnConfiguration");
		assertThat(config, notNullValue());
		assertThat(configuration, notNullValue());
		assertThat(config, sameInstance(configuration));

		assertThat(config.get("fs.defaultFS"), startsWith("hdfs"));
		assertThat(config.get("foo"), is("jee"));
	}

	@org.springframework.context.annotation.Configuration
	@EnableYarn(enable=Enable.BASE)
	static class Config extends SpringYarnConfigurerAdapter {

		@Override
		public void configure(YarnConfigConfigurer config) throws Exception {
			config
				.loadDefaults(false)
				.withProperties()
					.property("foo", "jee");
		}

	}

}
