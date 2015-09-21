/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.config.namespace;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.springframework.beans.factory.support.ManagedProperties;

/**
 * Properties hack to provide an ordered Properties object.
 *
 * @author Costin Leau
 */
@SuppressWarnings("serial")
public class LinkedProperties extends ManagedProperties {

	private final LinkedHashSet<Object> keys = new LinkedHashSet<Object>();

	LinkedProperties() {
		super();
	}

	LinkedProperties(String text) {
		super();
		try {
			this.load(new ByteArrayInputStream(text.getBytes("ISO-8859-1")));
		} catch (IOException ex) {
			// Should never happen.
			throw new IllegalArgumentException("Failed to parse [" + text + "] into Properties", ex);
		}
	}

	public Enumeration<Object> keys() {
		return Collections.<Object> enumeration(keys);
	}

	public Object put(Object key, Object value) {
		keys.add(key);
		return super.put(key, value);
	}

	public Set<String> stringPropertyNames() {
		Set<String> set = new LinkedHashSet<String>();

		for (Object key : this.keys) {
			if (key instanceof String && this.get(key) instanceof String)
				set.add((String) key);
		}

		return set;
	}

	public Set<Object> keySet() {
		return keys;
	}

	@Override
	public Object merge(Object parent) {
		if (!isMergeEnabled()) {
			throw new IllegalStateException("Not allowed to merge when the 'mergeEnabled' property is set to 'false'");
		}
		if (parent == null) {
			return this;
		}
		if (!(parent instanceof Properties)) {
			throw new IllegalArgumentException("Cannot merge with object of type [" + parent.getClass() + "]");
		}
		Properties merged = new LinkedProperties();
		merged.putAll((Properties) parent);
		merged.putAll(this);
		return merged;
	}
}
