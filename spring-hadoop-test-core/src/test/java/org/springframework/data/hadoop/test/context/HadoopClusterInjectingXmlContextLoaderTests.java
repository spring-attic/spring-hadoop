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
package org.springframework.data.hadoop.test.context;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Testing annotated mini cluster and configuration.
 *
 * @author Janne Valkealahti
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class HadoopClusterInjectingXmlContextLoaderTests {

	@Autowired
	private ApplicationContext ctx;

	@Resource(name = "hadoopConfiguration")
	Configuration configuration;

	@Test
	public void testLoaderAndConfig() {
		assertNotNull(ctx);
		assertTrue(ctx.containsBean("hadoopCluster"));
		assertTrue(ctx.containsBean("hadoopConfiguration"));
		Configuration config = (Configuration) ctx.getBean("hadoopConfiguration");
		assertNotNull(config);
	}

}
