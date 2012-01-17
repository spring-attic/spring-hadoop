/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.hadoop.fs;

import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ResourceLoader;

import static org.junit.Assert.*;

/**
 * @author Costin Leau
 */
public class ChainedResourceLoaderTest {

	public static class ResourceLoaderTestBean implements ApplicationContextAware, ResourceLoaderAware {
		public ApplicationContext ctx;
		public ResourceLoader loader;

		public void setResourceLoader(ResourceLoader resourceLoader) {
			this.loader = resourceLoader;
		}

		public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
			this.ctx = applicationContext;
		}
	}

	@Test
	public void testChainedRLWithGenericCtx() throws Exception {
		GenericXmlApplicationContext ctx = new GenericXmlApplicationContext(
				"/org/springframework/data/hadoop/fs/rl.xml");

		ctx.registerShutdownHook();

		ResourceLoaderTestBean testBean = ctx.getBean(ResourceLoaderTestBean.class);
		ChainedResourceLoader crl = ctx.getBean(ChainedResourceLoader.class);


		assertSame(ctx, testBean.ctx);
		assertSame(ctx, testBean.loader);
	}
}
