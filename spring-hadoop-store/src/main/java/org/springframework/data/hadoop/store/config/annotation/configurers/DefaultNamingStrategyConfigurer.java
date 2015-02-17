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
package org.springframework.data.hadoop.store.config.annotation.configurers;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer;
import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerAdapter;
import org.springframework.data.hadoop.store.config.annotation.SpringDataStoreWriterConfigs;
import org.springframework.data.hadoop.store.config.annotation.builders.DataStoreTextWriterBuilder;
import org.springframework.data.hadoop.store.config.annotation.builders.DataStoreTextWriterConfigurer;
import org.springframework.data.hadoop.store.strategy.naming.ChainedFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.CodecFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.RollingFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.UuidFileNamingStrategy;

/**
 * {@link AnnotationConfigurer} which knows howto configure a naming strategy.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultNamingStrategyConfigurer extends
		AnnotationConfigurerAdapter<SpringDataStoreWriterConfigs, DataStoreTextWriterConfigurer, DataStoreTextWriterBuilder> implements
		NamingStrategyConfigurer {

	private List<FileNamingStrategy> fileNamingStrategies = new ArrayList<FileNamingStrategy>();

	@Override
	public void configure(DataStoreTextWriterBuilder builder) throws Exception {
		if (fileNamingStrategies.size() == 1) {
			builder.setFileNamingStrategy(fileNamingStrategies.get(0));
		} else if (fileNamingStrategies.size() > 1) {
			builder.setFileNamingStrategy(new ChainedFileNamingStrategy(fileNamingStrategies));
		}
	}

	@Override
	public NamingStrategyConfigurer name(String name, String prefix) {
		fileNamingStrategies.add(new StaticFileNamingStrategy(name, prefix));
		return this;
	}

	@Override
	public NamingStrategyConfigurer name(String name) {
		fileNamingStrategies.add(new StaticFileNamingStrategy(name));
		return this;
	}

	@Override
	public NamingStrategyConfigurer rolling() {
		fileNamingStrategies.add(new RollingFileNamingStrategy());
		return this;
	}

	@Override
	public NamingStrategyConfigurer codec() {
		fileNamingStrategies.add(new CodecFileNamingStrategy());
		return this;
	}

	@Override
	public NamingStrategyConfigurer uuid() {
		fileNamingStrategies.add(new UuidFileNamingStrategy());
		return this;
	}

}
