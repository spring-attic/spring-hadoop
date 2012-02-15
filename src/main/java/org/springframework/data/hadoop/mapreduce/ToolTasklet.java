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
package org.springframework.data.hadoop.mapreduce;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.BeanUtils;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.util.Assert;

/**
 * Tasklet for executing Hadoop {@link Tool}s.
 * 
 * @author Costin Leau
 */
public class ToolTasklet implements Tasklet {

	private String[] arguments;
	private Configuration configuration;
	private Properties properties;
	private Tool tool;
	private Class<? extends Tool> toolClass;


	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		Tool t = (tool != null ? tool : BeanUtils.instantiateClass(toolClass));
		Configuration cfg = ConfigurationUtils.createFrom(configuration, properties);
		org.apache.hadoop.util.ToolRunner.run(cfg, t, arguments);

		return RepeatStatus.FINISHED;
	}

	/**
	 * Sets the tool.
	 *
	 * @param tool The tool to set.
	 */
	public void setTool(Tool tool) {
		Assert.isNull(toolClass, "a Tool class already set");
		this.tool = tool;
	}

	/**
	 * Sets the tool class.
	 *
	 * @param toolClass the new tool class
	 */
	public void setToolClass(Class<? extends Tool> toolClass) {
		Assert.isNull(tool, "a Tool instance already set");
		this.toolClass = toolClass;
	}

	/**
	 * Sets the arguments.
	 *
	 * @param arguments The arguments to set.
	 */
	public void setArguments(String[] arguments) {
		this.arguments = arguments;
	}

	/**
	 * Sets the configuration.
	 *
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the properties.
	 *
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
}