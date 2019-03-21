/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store.config.annotation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.ObjectPostProcessor;
import org.springframework.data.hadoop.store.config.annotation.builders.DataStoreTextWriterBuilder;
import org.springframework.data.hadoop.store.config.annotation.builders.DataStoreTextWriterConfigurer;
import org.springframework.data.hadoop.store.config.annotation.builders.SpringDataStoreTextWriterBuilder;

/**
 * Provides a convenient base class for creating a {@link SpringDataStoreTextWriterConfigurer}
 * instance. The implementation allows customization by overriding methods.
 *
 * @author Janne Valkealahti
 * @see EnableDataStoreTextWriter
 *
 */
public class SpringDataStoreTextWriterConfigurerAdapter implements SpringDataStoreTextWriterConfigurer {

	private DataStoreTextWriterBuilder hadoopConfigBuilder;

	private ObjectPostProcessor<Object> objectPostProcessor = new ObjectPostProcessor<Object>() {
		@Override
		public <T> T postProcess(T object) {
			throw new IllegalStateException(ObjectPostProcessor.class.getName()
					+ " is a required bean. Ensure you have used @EnableDataStoreWriter and @Configuration");
		}
	};

	@Override
	public final void init(SpringDataStoreTextWriterBuilder builder) throws Exception {
		builder.setSharedObject(DataStoreTextWriterBuilder.class, getConfigBuilder());
	}

	@Override
	public void configure(SpringDataStoreTextWriterBuilder builder) throws Exception {
	}

	@Override
	public boolean isAssignable(AnnotationBuilder<SpringDataStoreWriterConfigs> builder) {
		return true;
	}

	@Override
	public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	}

	@Autowired
	public void setObjectPostProcessor(ObjectPostProcessor<Object> objectPostProcessor) {
		this.objectPostProcessor = objectPostProcessor;
	}

	protected final DataStoreTextWriterBuilder getConfigBuilder() throws Exception {
		if (hadoopConfigBuilder != null) {
			return hadoopConfigBuilder;
		}
		hadoopConfigBuilder = new DataStoreTextWriterBuilder(objectPostProcessor);
		configure(hadoopConfigBuilder);
		return hadoopConfigBuilder;
	}

}
