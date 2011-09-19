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
package org.springframework.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.springframework.core.convert.ConversionService;

/**
 * @author Dave Syer
 * 
 */
public class ConversionServiceRecordWriter extends RecordWriter<Object, Object> {

	private final TaskInputOutputContext<Writable, Writable, ? super Writable, ? super Writable> target;

	private final ConversionService conversionService;

	private final Class<? extends Writable> outputKeyType;

	private final Class<? extends Writable> outputValueType;

	public ConversionServiceRecordWriter(TaskInputOutputContext<Writable, Writable, ? super Writable, ? super Writable> target,
			ConversionService conversionService, Class<? extends Writable> outputKeyType,
			Class<? extends Writable> outputValueType) {
		this.target = target;
		this.conversionService = conversionService;
		this.outputKeyType = outputKeyType;
		this.outputValueType = outputValueType;
	}

	@Override
	public void write(Object key, Object value) throws IOException {
		Writable writableKey = conversionService.convert(key, outputKeyType);
		Writable writableValue = conversionService.convert(value, outputValueType);
		try {
			target.write(writableKey, writableValue);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException("Interrupted writing.", e);
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
	}

}
