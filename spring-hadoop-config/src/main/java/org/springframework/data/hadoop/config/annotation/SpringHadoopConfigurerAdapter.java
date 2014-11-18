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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigBuilder;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigConfigurer;
import org.springframework.data.hadoop.config.annotation.builders.SpringHadoopConfigBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.ObjectPostProcessor;

/**
 * Provides a convenient base class for creating a {@link SpringHadoopConfigurer}
 * instance. The implementation allows customization by overriding methods.
 *
 * @author Janne Valkealahti
 * @see EnableHadoop
 *
 */
public class SpringHadoopConfigurerAdapter implements SpringHadoopConfigurer {

	private HadoopConfigBuilder hadoopConfigBuilder;

	private ObjectPostProcessor<Object> objectPostProcessor = new ObjectPostProcessor<Object>() {
		@Override
		public <T> T postProcess(T object) {
			throw new IllegalStateException(ObjectPostProcessor.class.getName()
					+ " is a required bean. Ensure you have used @EnableYarn and @Configuration");
		}
	};

	@Override
	public final void init(SpringHadoopConfigBuilder builder) throws Exception {
		builder.setSharedObject(HadoopConfigBuilder.class, getConfigBuilder());
	}

	@Override
	public void configure(SpringHadoopConfigBuilder builder) throws Exception {
	}

	@Override
	public boolean isAssignable(AnnotationBuilder<SpringHadoopConfigs> builder) {
		return true;
	}

	@Override
	public void configure(HadoopConfigConfigurer config) throws Exception {
	}

	@Autowired(required=false)
	public void setObjectPostProcessor(ObjectPostProcessor<Object> objectPostProcessor) {
		this.objectPostProcessor = objectPostProcessor;
	}

	protected final HadoopConfigBuilder getConfigBuilder() throws Exception {
		if (hadoopConfigBuilder != null) {
			return hadoopConfigBuilder;
		}
		hadoopConfigBuilder = new HadoopConfigBuilder(objectPostProcessor);
		configure(hadoopConfigBuilder);
		return hadoopConfigBuilder;
	}

}
