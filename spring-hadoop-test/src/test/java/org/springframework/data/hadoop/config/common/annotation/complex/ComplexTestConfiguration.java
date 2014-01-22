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
package org.springframework.data.hadoop.config.common.annotation.complex;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.config.common.annotation.AbstractAnnotationConfiguration;
import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer;
import org.springframework.data.hadoop.config.common.annotation.simple.SimpleTestConfig;

/**
 * @{@link Configuration} which is imported from @{@ EnableSimpleTest}.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
public class ComplexTestConfiguration extends AbstractAnnotationConfiguration<ComplexTestConfigBuilder, ComplexTestConfig> {

	ComplexTestConfigBuilder builder = new ComplexTestConfigBuilder();

	@Autowired(required=false)
	@Qualifier("simpleConfig")
	SimpleTestConfig simpleTestConfig;

	@Bean(name="complexConfig")
	public ComplexTestConfig complexTestConfig() {
		ComplexTestConfig config = builder.getOrBuild();
		config.simpleTestConfig = simpleTestConfig;
		return config;
	}

	@Bean(name="complexConfigData")
	public String complexTestConfigData() {
		ComplexTestConfig config = builder.getOrBuild();
		return config.complexData;
	}

	@Bean(name="complexConfigBeanB")
	public ComplexTestConfigBeanB complexTestConfigBeanB() {
		ComplexTestConfig config = builder.getOrBuild();
		return config.complexBeanB;
	}

	@Override
	protected void onConfigurers(List<AnnotationConfigurer<ComplexTestConfig, ComplexTestConfigBuilder>> configurers)	throws Exception {
		for (AnnotationConfigurer<ComplexTestConfig, ComplexTestConfigBuilder> configurer : configurers) {
			if (configurer.isAssignable(builder)) {
				builder.apply(configurer);
			}
		}
	}

}