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
package org.springframework.data.hadoop.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.core.convert.ConversionService;

/**
 * @author Dave Syer
 * 
 */
public class ReducerFactoryBean extends AbstractExpressionEvaluatorFactoryBean<Reducer<?, ?, ?, ?>> {

	private Class<?> targetValueType;

	public void setInputValueType(Class<?> inputValueType) {
		this.targetValueType = inputValueType;
	}

	@Override
	protected Reducer<?, ?, ?, ?> doGetObject(Object target, ConversionService conversionService,
			Class<? extends Writable> outputKeyType, Class<? extends Writable> outputValueType) {
		ExpressionFactory factory = new ExpressionFactory();
		String expression = factory.getReducerExpression(target);
		if (targetValueType == null) {
			targetValueType = outputValueType;
		}
		return new ExpressionEvaluatingReducer<Object>(target, expression, conversionService, outputKeyType,
				outputValueType, targetValueType);
	}

	@Override
	protected Reducer<?, ?, ?, ?> doGetObject(Object target, String method, ConversionService conversionService,
			Class<? extends Writable> outputKeyType, Class<? extends Writable> outputValueType) {
		ExpressionFactory factory = new ExpressionFactory();
		String expression = factory.getReducerExpression(target, method);
		if (targetValueType == null) {
			targetValueType = outputValueType;
		}
		return new ExpressionEvaluatingReducer<Writable>(target, expression, conversionService, outputKeyType,
				outputValueType, targetValueType);
	}

	@Override
	public Class<?> getObjectType() {
		return Reducer.class;
	}

}
