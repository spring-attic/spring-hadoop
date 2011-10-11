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
package org.springframework.data.hadoop.convert;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalGenericConverter;
import org.springframework.data.hadoop.HadoopException;

/**
 * @author Dave Syer
 *
 */
public class StreamingMapToMapConverter implements ConditionalGenericConverter {

	private final ConversionService conversionService;

	public StreamingMapToMapConverter(ConversionService conversionService) {
		this.conversionService = conversionService;
	}

	public Set<ConvertiblePair> getConvertibleTypes() {
		return Collections.singleton(new ConvertiblePair(Map.class, Map.class));
	}

	public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
		return this.conversionService.canConvert(sourceType.getMapKeyTypeDescriptor(), targetType.getMapKeyTypeDescriptor()) && 
			this.conversionService.canConvert(sourceType.getMapValueTypeDescriptor(), targetType.getMapValueTypeDescriptor());
	}

	@SuppressWarnings("unchecked")
	public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
		if (source == null) {
			return null;
		}
		Map<Object, Object> sourceMap = (Map<Object, Object>) source;
		if (sourceMap.isEmpty()) {
			return sourceMap;
		}
		Map<Object, Object> targetMap = new StreamingMap(sourceType, targetType, sourceMap);
		return targetMap;
	}

	private class StreamingMap implements Map<Object, Object> {

		private final TypeDescriptor sourceType;
		private final TypeDescriptor targetType;
		private final Map<Object, Object> sourceMap;

		private Object key;
		private Object value;
		private int size;
		
		public StreamingMap(TypeDescriptor sourceType, TypeDescriptor targetType, Map<Object, Object> sourceMap) {
			this.sourceType = sourceType;
			this.targetType = targetType;
			this.sourceMap = sourceMap;
		}

		public int size() {
			return size;
		}

		public boolean isEmpty() {
			return size>0;
		}

		public boolean containsKey(Object key) {
			throw new UnsupportedOperationException();
		}

		public boolean containsValue(Object value) {
			throw new UnsupportedOperationException();
		}

		public Object get(Object key) {
			throw new UnsupportedOperationException();
		}

		public Object put(Object key, Object value) {
			Object targetKey = conversionService.convert(key, sourceType.getMapKeyTypeDescriptor(key),
					targetType.getMapKeyTypeDescriptor(key));
			Object targetValue = conversionService.convert(value, sourceType.getMapValueTypeDescriptor(value),
					targetType.getMapValueTypeDescriptor(value));
			this.key = key;
			this.value = value;
			this.size++;
			try {
				return sourceMap.put(targetKey, targetValue);
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				throw new HadoopException("Could not write to record writer", e);
			}
		}

		public Object remove(Object key) {
			throw new UnsupportedOperationException();
		}

		public void putAll(Map<? extends Object, ? extends Object> m) {
			for (Entry<? extends Object, ? extends Object> entry : m.entrySet()) {
				put(entry.getKey(), entry.getValue());
			}
		}

		public void clear() {
			this.key = null;
			this.value = null;
			this.size = 0;
		}

		public Set<Object> keySet() {
			return Collections.singleton(key);
		}

		public Collection<Object> values() {
			return Collections.singleton(value);
		}

		public Set<Entry<Object, Object>> entrySet() {
			throw new UnsupportedOperationException();
		}
		
	}

}
