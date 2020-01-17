/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.hbase;

import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * Base class for {@link HbaseTemplate} and {@link HbaseInterceptor}, defining commons properties such as {@link HTableInterfaceFactory} and {@link Configuration}.
 * 
 * Not intended to be used directly.
 * 
 * @author Costin Leau
 */
public abstract class HbaseAccessor implements InitializingBean {

	private String encoding;
	private Charset charset = HbaseUtils.getCharset(encoding);

	private Configuration configuration;

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(configuration, " a valid configuration is required");
		// detect charset
		charset = HbaseUtils.getCharset(encoding);
	}

	/**
	 * Sets the encoding.
	 *
	 * @param encoding The encoding to set.
	 */
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	/**
	 * Sets the configuration.
	 *
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public Charset getCharset() {
		return charset;
	}

	public Configuration getConfiguration() {
		return configuration;
	}
}