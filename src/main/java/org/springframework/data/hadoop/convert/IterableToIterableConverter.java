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
package org.springframework.data.hadoop.convert;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalGenericConverter;
import org.springframework.util.ReflectionUtils;

/**
 * @author Dave Syer
 *
 */
public class IterableToIterableConverter implements ConditionalGenericConverter {

	private final ConversionService conversionService;

	public IterableToIterableConverter(ConversionService conversionService) {
		this.conversionService = conversionService;
	}

	public Set<ConvertiblePair> getConvertibleTypes() {
		return Collections.singleton(new ConvertiblePair(Iterable.class, Iterable.class));
	}

	public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
		return this.conversionService.canConvert(sourceType.getElementTypeDescriptor(), targetType.getElementTypeDescriptor());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
		if (source == null) {
			return null;
		}
		Iterable<?> sourceCollection = (Iterable<?>) source;
		if (sourceCollection.iterator().hasNext()) {
			return sourceCollection;
		}
		return new IterableAdapter((Iterable) source, Object.class);
	}

	private static class IterableAdapter<I, O> implements Iterable<O> {
		private final Iterable<I> values;
		private final Method method;

		public IterableAdapter(Iterable<I> values, Class<? extends I> inputType) {
			this.values = values;
			method = ReflectionUtils.findMethod(inputType, "get");
		}

		public Iterator<O> iterator() {
			return new Iterator<O>() {

				private Iterator<I> wrapped = values.iterator();

				public boolean hasNext() {
					return wrapped.hasNext();
				}

				@SuppressWarnings("unchecked")
				public O next() {
					return (O) ReflectionUtils.invokeMethod(method, wrapped.next());
				}

				public void remove() {
					wrapped.remove();
				}
			};
		}
	}
}