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
package org.springframework.data.hadoop.boot;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.HadoopSystemConstants;
import org.springframework.data.hadoop.config.annotation.EnableHadoop;
import org.springframework.data.hadoop.fs.FsShell;

/**
 * Boot auto-configuration for {@link FsShell}.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@ConditionalOnClass(EnableHadoop.class)
@ConditionalOnMissingBean(FsShell.class)
@AutoConfigureAfter(HadoopAutoConfiguration.class)
public class HadoopFsShellAutoConfiguration {

	@Autowired
	private org.apache.hadoop.conf.Configuration configuration;

	@Bean(name = HadoopSystemConstants.DEFAULT_ID_FSSHELL)
	@ConditionalOnExpression("${spring.hadoop.fsshell.enabled:true}")
	public FsShell fsShell() {
		return new FsShell(configuration);
	}

}
