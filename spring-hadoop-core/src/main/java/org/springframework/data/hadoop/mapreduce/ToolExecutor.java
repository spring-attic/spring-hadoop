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
package org.springframework.data.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.springframework.util.Assert;

/**
 * Customized executor for {@link Tool}.
 * 
 * @author Costin Leau
 */
public abstract class ToolExecutor extends HadoopCodeExecutor<Tool> {

	@Override
	protected Integer invokeTargetObject(Configuration cfg, Tool target, Class<Tool> targetClass, String[] args) throws Exception {
		return org.apache.hadoop.util.ToolRunner.run(cfg, target, args);
	}

	protected Class<Tool> loadClass(String className, ClassLoader cl) {
		Class<Tool> clazz = super.loadClass(className, cl);
		Assert.isAssignable(Tool.class, clazz, "Class [" + clazz + "] is not a Tool instance.");
		return clazz;
	}

	/**
	 * Sets the tool.
	 *
	 * @param tool The tool to set.
	 */
	public void setTool(Tool tool) {
		Assert.isNull(targetClassName, "a Tool class already set");
		setTargetObject(tool);
	}

	/**
	 * Sets the tool class by name.
	 *
	 * @param toolClassName the new tool class
	 */
	public void setToolClass(String toolClassName) {
		Assert.isNull(target, "a Tool instance already set");
		setTargetClassName(toolClassName);
	}
}