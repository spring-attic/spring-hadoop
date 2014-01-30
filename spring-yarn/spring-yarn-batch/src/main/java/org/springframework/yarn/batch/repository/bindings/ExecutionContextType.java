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
package org.springframework.yarn.batch.repository.bindings;

import java.util.Map;

import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

/**
 * Binding for {@link org.springframework.batch.item.ExecutionContext}.
 * We need to do a trick to also store clazz type of objects in
 * a map because otherwise json desirialization may take wrong
 * type i.e. between Integer and Long.
 *
 * @author Janne Valkealahti
 *
 */
public class ExecutionContextType extends BaseObject {

	public Map<String, ObjectEntry> map;

	public ExecutionContextType() {
		super("ExecutionContextType");
	}

	public static class ObjectEntry {
		public Object obj;
		public String clazz;
		public ObjectEntry(){}
		public ObjectEntry(Object obj, String clazz) {
			this.obj = obj;
			this.clazz = clazz;
		}
	}

}
