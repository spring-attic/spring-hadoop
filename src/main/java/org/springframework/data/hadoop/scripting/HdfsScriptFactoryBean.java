/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.scripting;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.hadoop.fs.SimplerFileSystem;
import org.springframework.data.hadoop.io.HdfsResourceLoader;
import org.springframework.util.Assert;

/**
 * Hadoop-customized factory that exposes Hadoop specific variables to scripting languages.
 * The instances exposes are reused from the enclosing context (using naming conventions or autowiring strategies)
 * or created on demand (in case of lightweight objects).
 * <p/>
 * These are :
 * 
 * <table>
 *  <th>
 *  <tr>
 *    <td>Name</td><td>Type</td><td>Description</td>
 *  </tr>
 *  </th>
 *  <tr><td>cfg</td><td> org.apache.hadoop.conf.Configuration</td><td>Hadoop Configuration (relies on 'hadoop-configuration' bean or singleton type match)</td></tr>
 * 	<tr><td>fs</td><td>org.apache.hadoop.fs.FileSystem</td><td>Hadoop File System (relies on 'hadoop-fs' bean or singleton type match, falls back to creating one based on 'cfg')</td></tr>
 *  <tr><td>hdfsRL</td><td>org.springframework.data.hadoop.io.HdfsResourceLoader</td><td>HdfsResourceLoader (relies on 'hadoop-resource-loader' or singleton type match, falls back to creating one automatically based on 'cfg')</td></tr>
 *  <tr><td>ctx</td><td>org.springframework.context.ApplicationContext</td><td>Enclosing application context</td></tr>
 *  <tr><td>ctxRL</td><td>org.springframework.io.support.ResourcePatternResolver</td><td>Enclosing application context ResourceLoader (same as ctx)</td></tr>
 *  <tr><td>cl</td><td>java.lang.ClassLoader</td><td>ClassLoader used for executing this script</td></tr>
 * </table>
 * 
 * <p/>
 * Note that the above variables are added only if found (have a non-null value) and the keys are not bound already.
 * 
 * @author Costin Leau
 */
public class HdfsScriptFactoryBean extends Jsr223ScriptEvaluatorFactoryBean implements ApplicationContextAware {

	private static final Log log = LogFactory.getLog(HdfsScriptFactoryBean.class);

	private ApplicationContext ctx;

	@Override
	protected void postProcess(Map<String, Object> args) {
		// rather ugly initialization
		// forced to postpone as much as possible the instance lookup
		// if not needed

		String name = "cfg";

		if (!hasBinding(args, name, Configuration.class)) {
			putIfAbsent(args, name, detectCfg(name));
		}

		Configuration cfg = (Configuration) args.get(name);

		name = "hdfsRL";

		if (!hasBinding(args, name, HdfsResourceLoader.class)) {
			putIfAbsent(args, name, detectHdfsRL(name, cfg));
		}

		name = "fs";

		if (!hasBinding(args, name, FileSystem.class)) {
			putIfAbsent(args, name, detectFS(name, cfg));
		}

		putIfAbsent(args, "cl", ctx.getClassLoader());
		putIfAbsent(args, "ctxRL", ctx);
		putIfAbsent(args, "ctx", ctx);
	}

	private boolean hasBinding(Map<String, Object> args, String key, Class<?> type) {
		if (args.containsKey(key)) {
			Assert.isInstanceOf(type, args.get(key), "Invalid property '" + key + "' ");
		}
		return false;
	}

	private Configuration detectCfg(String variableName) {
		String defaultName = "hadoop-configuration";
		Class<Configuration> defaultType = Configuration.class;

		if (ctx.containsBean(defaultName))
			return ctx.getBean(defaultName, defaultType);
		String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(ctx, defaultType);
		if (names != null && names.length == 1) {
			return ctx.getBean(defaultName, defaultType);
		}

		if (log.isDebugEnabled()) {
			log.debug(String.format(
					"Cannot find bean '%s' or a unique bean of type '%s'; not binding variable '%s' to script",
					defaultName, defaultType, variableName));
		}
		return null;
	}

	private HdfsResourceLoader detectHdfsRL(String variableName, Configuration cfg) {
		String defaultName = "hadoop-resource-loader";
		Class<HdfsResourceLoader> defaultType = HdfsResourceLoader.class;

		if (ctx.containsBean(defaultName))
			return ctx.getBean(defaultName, defaultType);
		String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(ctx, defaultType);
		if (names != null && names.length == 1) {
			return ctx.getBean(defaultName, defaultType);
		}

		// create one instance
		return new HdfsResourceLoader(cfg);
	}


	private Object detectFS(String variableName, Configuration detectedCfg) {
		String defaultName = "hadoop-fs";
		Class<?> defaultType = FileSystem.class;

		if (ctx.containsBean(defaultName))
			return ctx.getBean(defaultName, defaultType);
		String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(ctx, defaultType);
		if (names != null && names.length == 1) {
			return ctx.getBean(defaultName, defaultType);
		}

		try {
			FileSystem fs = FileSystem.get(detectedCfg);
			return (fs instanceof SimplerFileSystem ? fs : new SimplerFileSystem(fs));
		} catch (IOException ex) {
			log.warn(String.format("Cannot create HDFS file system'; not binding variable '%s' to script", 
					defaultName, defaultType, variableName), ex);
		}

		return null;
	}

	private void putIfAbsent(Map<String, Object> arguments, String key, Object value) {
		if (value != null && !arguments.containsKey(key)) {
			arguments.put(key, value);
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.ctx = applicationContext;
	}
}