/*
 * Copyright 2006-2011 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.hadoop.mapreduce;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.core.convert.ConversionService;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.expression.spel.support.StandardTypeConverter;
import org.springframework.hadoop.util.ConversionServiceIterableAdapter;

/**
 * @author Dave Syer
 * 
 */
public class ExpressionEvaluatingReducer<I> extends Reducer<Writable, Writable, Writable, Writable> {

	private final Object target;

	private final StandardEvaluationContext context = new StandardEvaluationContext();

	private Expression expression;

	private final ConversionService conversionService;

	private final Class<? extends Writable> outputKeyType;

	private final Class<? extends Writable> outputValueType;

	private final Class<?> targetValueType;

	public ExpressionEvaluatingReducer(Object target, String expression, ConversionService conversionService,
			Class<? extends Writable> outputKeyType, Class<? extends Writable> outputValueType, Class<?> targetValueType) {
		this.target = target;
		this.conversionService = conversionService;
		this.outputKeyType = outputKeyType;
		this.outputValueType = outputValueType;
		this.targetValueType = targetValueType;
		this.context.setTypeConverter(new StandardTypeConverter(conversionService));
		this.expression = new SpelExpressionParser().parseExpression(expression);
	}

	@Override
	protected void reduce(Writable key, Iterable<Writable> values, Context context) throws IOException,
			InterruptedException {

		this.context.setVariable("target", target);

		ParametersWrapper wrapper = new ParametersWrapper(key, values, this.targetValueType, context,
				this.conversionService, this.outputKeyType, this.outputValueType);
		Object value = this.expression.getValue(this.context, wrapper);

		if (value == null) {
			return;
		}

		RecordWriter<Object, Object> writer = wrapper.getWriter();

		// Client can return a map of output values
		if (value instanceof Map<?, ?>) {
			for (Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
				writer.write(entry.getKey(), entry.getValue());
			}
			return;
		}

		// Assume return value is the output
		writer.write(key, value);

	}

	@SuppressWarnings("unused")
	private static class ParametersWrapper {

		private final Writable key;

		private final Reducer<Writable, Writable, Writable, Writable>.Context context;

		private final RecordWriter<Object, Object> writer;

		private Iterable<?> values;

		public ParametersWrapper(Writable key, Iterable<Writable> values, Class<?> targetValueType,
				Reducer<Writable, Writable, Writable, Writable>.Context context, ConversionService conversionService,
				Class<? extends Writable> outputKeyType, Class<? extends Writable> outputValueType) {
			this.key = key;
			this.context = context;
			this.values = new ConversionServiceIterableAdapter<Writable, Object>(values, targetValueType,
					conversionService);
			this.writer = new ConversionServiceRecordWriter(context, conversionService, outputKeyType, outputValueType);
		}

		public Writable getKey() {
			return key;
		}

		public Reducer<Writable, Writable, Writable, Writable>.Context getContext() {
			return context;
		}

		public RecordWriter<Object, Object> getWriter() {
			return writer;
		}

		public Iterable<?> getValues() {
			return values;
		}

	}

}