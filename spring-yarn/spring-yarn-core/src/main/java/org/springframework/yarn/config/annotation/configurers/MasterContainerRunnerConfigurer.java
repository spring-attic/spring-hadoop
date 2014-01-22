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
package org.springframework.yarn.config.annotation.configurers;

import java.util.Properties;

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerAdapter;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterBuilder;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterConfigure;
import org.springframework.yarn.container.CommandLineContainerRunner;
import org.springframework.yarn.launch.AbstractCommandLineRunner;
import org.springframework.yarn.launch.LaunchCommandsFactoryBean;

/**
 *
 *
 * @author Janne Valkealahti
 *
 */
public class MasterContainerRunnerConfigurer
		extends AnnotationConfigurerAdapter<YarnAppmaster, YarnAppmasterConfigure, YarnAppmasterBuilder>
		implements MasterContainerRunnerConfigure {

	private Class<?> contextClass;
	private String contextFile = "container-context.xml";
	private String stdout = "<LOG_DIR>/Container.stdout";
	private String stderr = "<LOG_DIR>/Container.stderr";
	private String beanName = YarnSystemConstants.DEFAULT_ID_CONTAINER;
	private Class<? extends AbstractCommandLineRunner<?>> runnerClazz = CommandLineContainerRunner.class;

	private Properties arguments = new Properties();

	@Override
	public void configure(YarnAppmasterBuilder builder) throws Exception {
		LaunchCommandsFactoryBean fb = new LaunchCommandsFactoryBean();
		fb.setRunner(runnerClazz);
		fb.setContextFile(contextClass != null ?  contextClass.getCanonicalName() : contextFile);
		fb.setBeanName(beanName);

		fb.setArguments(arguments);

		fb.setStdout(stdout);
		fb.setStderr(stderr);
		fb.afterPropertiesSet();
		builder.containerCommands(fb.getObject());
	}

	@Override
	public void init(YarnAppmasterBuilder builder) throws Exception {
		super.init(builder);
	}

	@Override
	public MasterContainerRunnerConfigure contextClass(Class<?> contextClass) {
		this.contextClass = contextClass;
		return this;
	}

	@Override
	public MasterContainerRunnerConfigure contextFile(String contextFile) {
		this.contextFile = contextFile;
		return this;
	}

	@Override
	public MasterContainerRunnerConfigure stdout(String stdout) {
		this.stdout = stdout;
		return this;
	}

	@Override
	public MasterContainerRunnerConfigure stderr(String stderr) {
		this.stderr = stderr;
		return this;
	}

	@Override
	public MasterContainerRunnerConfigure beanName(String beanName) {
		this.beanName = beanName;
		return this;
	}

	@Override
	public MasterContainerRunnerConfigure runnerClass(Class<? extends AbstractCommandLineRunner<?>> runnerClazz) {
		this.runnerClazz = runnerClazz;
		return this;
	}

	@Override
	public MasterContainerRunnerConfigure arguments(Properties arguments) {
		this.arguments.putAll(arguments);
		return this;
	}

	@Override
	public MasterContainerRunnerConfigure argument(String key, String value) {
		this.arguments.put(key, value);
		return this;
	}

}
