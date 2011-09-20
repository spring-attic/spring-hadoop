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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.springframework.data.hadoop.HadoopException;

/**
 * @author Dave Syer
 * 
 */
public class RecordWriterMap implements Map<Object, Object> {

	private final RecordWriter<Object, Object> writer;
	private Object key;
	private Object value;
	private int size;

	public RecordWriterMap(RecordWriter<Object, Object> writer) {
		this.writer = writer;
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
		this.key = key;
		this.value = value;
		this.size++;
		try {
			writer.write(key, value);
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new HadoopException("Could not write to record writer", e);
		}
		return null;
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
