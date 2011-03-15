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
package org.springframework.hadoop.convert;

import java.util.Map;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.hadoop.convert.basics.BooleanBooleanWritableConverter;
import org.springframework.hadoop.convert.basics.BooleanWritableBooleanConverter;
import org.springframework.hadoop.convert.basics.ByteByteWritableConverter;
import org.springframework.hadoop.convert.basics.ByteWritableByteConverter;
import org.springframework.hadoop.convert.basics.DoubleDoubleWritableConverter;
import org.springframework.hadoop.convert.basics.DoubleWritableDoubleConverter;
import org.springframework.hadoop.convert.basics.FloatFloatWritableConverter;
import org.springframework.hadoop.convert.basics.FloatWritableFloatConverter;
import org.springframework.hadoop.convert.basics.IntWritableIntegerConverter;
import org.springframework.hadoop.convert.basics.IntegerIntWritableConverter;
import org.springframework.hadoop.convert.basics.LongLongWritableConverter;
import org.springframework.hadoop.convert.basics.LongWritableLongConverter;
import org.springframework.hadoop.convert.basics.StringTextConverter;
import org.springframework.hadoop.convert.basics.TextStringConverter;

/**
 * @author Dave Syer
 *
 */
public class HadoopConversionServiceFactory {

	public static GenericConversionService createDefaultConversionService() {
		GenericConversionService service = org.springframework.core.convert.support.ConversionServiceFactory.createDefaultConversionService();
        // basics converters
		service.addConverter(new BooleanWritableBooleanConverter());
        service.addConverter(new BooleanBooleanWritableConverter());
		service.addConverter(new ByteWritableByteConverter());
        service.addConverter(new ByteByteWritableConverter());
		service.addConverter(new IntWritableIntegerConverter());
		service.addConverter(new IntegerIntWritableConverter());
	    service.addConverter(new LongWritableLongConverter());
	    service.addConverter(new LongLongWritableConverter());
        service.addConverter(new FloatWritableFloatConverter());
        service.addConverter(new FloatFloatWritableConverter());
        service.addConverter(new DoubleWritableDoubleConverter());
        service.addConverter(new DoubleDoubleWritableConverter());
		service.addConverter(new TextStringConverter());
		service.addConverter(new StringTextConverter());
		// other converters
		service.addConverter(new IterableCollectionConverter());
		service.removeConvertible(Map.class, Map.class);
		service.addConverter(new StreamingMapToMapConverter(service));
		return service;
	}


	public static void registerConverters(Set<Converter<?, ?>> converters, ConverterRegistry registry) {
		org.springframework.core.convert.support.ConversionServiceFactory.registerConverters(converters, registry);
	}

}
