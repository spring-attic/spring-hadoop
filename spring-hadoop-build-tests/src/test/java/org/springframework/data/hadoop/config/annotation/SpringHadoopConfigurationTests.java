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
package org.springframework.data.hadoop.config.annotation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigConfigurer;

public class SpringHadoopConfigurationTests {

	@Test
	public void testConfig() throws Exception {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(Config.class);
		assertTrue(ctx.containsBean("hadoopConfiguration"));
		org.apache.hadoop.conf.Configuration configuration = ctx.getBean("hadoopConfiguration", org.apache.hadoop.conf.Configuration.class);
		assertThat(configuration.get("fs.defaultFS"), is("hdfs://localhost:8021"));
		assertNotNull(configuration);
		ctx.close();
	}

	@Configuration
	@EnableHadoop
	static class Config extends SpringHadoopConfigurerAdapter {

		@Override
		public void configure(HadoopConfigConfigurer config) throws Exception {
			config
				.fileSystemUri("hdfs://localhost:8021");
		}

	}

}
