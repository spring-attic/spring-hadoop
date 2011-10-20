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
package org.springframework.data.hadoop.batch;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

/**
 * Basic item writer relying on {@link WritableResource}. Uses the provided {@link ResourceLoader}
 * to resolve the resulting resources based on the given {@link NameGenerator}. 
 * If no generator is specified, the resource URI will be used instead for resolving the writable resource.  
 * 
 * Copies the given resources to the generated ones.
 * 
 * @author Costin Leau
 */
public class ResourcesItemWriter implements InitializingBean, ItemWriter<Resource>, ApplicationContextAware {

	public interface OutputStreamDecorator {
		OutputStream decorate(OutputStream out) throws IOException;
	}

	public interface InputStreamDecorator {
		InputStream decorate(InputStream in) throws IOException;
	}

	private static final Log log = LogFactory.getLog(ResourcesItemWriter.class);

	private ResourceLoader resourceLoader;

	private NameGenerator generator;

	private boolean overwrite = false;

	private ApplicationContext ctx;

	private InputStreamDecorator inDecorator;
	private OutputStreamDecorator outDecorator;

	public void afterPropertiesSet() {
		Assert.isTrue(resourceLoader != null || ctx != null, "a resource loader is required");
		if (resourceLoader == null) {
			resourceLoader = ctx;
		}
	}

	public void setResourceLoader(ResourceLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.ctx = applicationContext;
	}

	public void setGenerator(NameGenerator generator) {
		this.generator = generator;
	}


	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}

	public void write(List<? extends Resource> items) throws Exception {
		boolean trace = log.isTraceEnabled();

		for (Resource resource : items) {
			String uri = resource.getURI().toString();
			String newUri = (generator != null ? generator.generate(uri) : uri);

			Resource out = resourceLoader.getResource(newUri);
			if (!out.exists() || overwrite) {
				Assert.isTrue(out instanceof WritableResource, "Cannot resolve a writable resource for " + newUri);
				WritableResource wOut = (WritableResource) out;
				Assert.isTrue(wOut.isWritable(), "Writable resources [" + wOut + "] is read-only");
				if (!out.equals(resource)) {
					InputStream inStream = resource.getInputStream();
					OutputStream outStream = wOut.getOutputStream();

					if (inDecorator != null) {
						inStream = inDecorator.decorate(inStream);
					}

					if (outDecorator != null) {
						outStream = outDecorator.decorate(outStream);
					}

					FileCopyUtils.copy(inStream, outStream);
				}
			}
			else {
				if (trace) {
					log.trace("Skipping writing resource [" + out.getDescription()
							+ "] since it already exists and overwriting not allowed");
				}
			}
		}
	}

	public void setInputStreamDecorator(InputStreamDecorator inDecorator) {
		this.inDecorator = inDecorator;
	}

	public void setOutputStreamDecorator(OutputStreamDecorator outDecorator) {
		this.outDecorator = outDecorator;
	}
}