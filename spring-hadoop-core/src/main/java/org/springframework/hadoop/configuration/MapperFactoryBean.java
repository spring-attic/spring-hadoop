/*
 * Copyright 2006-2011 the original author or authors.
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
package org.springframework.hadoop.configuration;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.core.convert.ConversionService;
import org.springframework.hadoop.mapreduce.ExpressionEvaluatingMapper;

/**
 * @author Dave Syer
 * 
 */
public class MapperFactoryBean extends AbstractExpressionEvaluatorFactoryBean<Mapper<?, ?, ?, ?>> {

	protected Mapper<?, ?, ?, ?> doGetObject(Object target, ConversionService conversionService,
			Class<? extends Writable> outputKeyType, Class<? extends Writable> outputValueType) {
		ExpressionFactory factory = new ExpressionFactory();
		String expression = factory.getMapperExpression(target);
		return new ExpressionEvaluatingMapper(target, expression, conversionService, outputKeyType, outputValueType);
	}

	@Override
	protected Mapper<?, ?, ?, ?> doGetObject(Object target, String method, ConversionService conversionService,
			Class<? extends Writable> outputKeyType, Class<? extends Writable> outputValueType) {
		ExpressionFactory factory = new ExpressionFactory();
		String expression = factory.getMapperExpression(target, method);
		return new ExpressionEvaluatingMapper(target, expression, conversionService, outputKeyType, outputValueType);
	}

	@Override
	public Class<?> getObjectType() {
		return Mapper.class;
	}

}
