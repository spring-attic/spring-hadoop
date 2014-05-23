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
package org.springframework.data.hadoop.store.expression;

import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;
import org.springframework.messaging.Message;

/**
 * A {@link PropertyAccessor} reading values from a backing {@link Message} used by a
 * partition key.
 *
 * @author Janne Valkealahti
 *
 */
public class MessagePartitionKeyPropertyAccessor implements PropertyAccessor {

	private final static Class<?>[] CLASSES = new Class[] { Message.class };

	private final static String PAYLOAD = "payload";

	private final static String HEADERS = "headers";

	@Override
	public Class<?>[] getSpecificTargetClasses() {
		return CLASSES;
	}

	@Override
	public boolean canRead(EvaluationContext context, Object target, String name) throws AccessException {
		if (target instanceof Message) {
			if (PAYLOAD.equals(name) || HEADERS.equals(name)) {
				return true;
			} else {
				return ((Message<?>) target).getHeaders().containsKey(name);
			}
		}
		return false;
	}

	@Override
	public TypedValue read(EvaluationContext context, Object target, String name) throws AccessException {
		if (target instanceof Message) {
			if (PAYLOAD.equals(name)) {
				return new TypedValue(((Message<?>) target).getPayload());
			} else if (HEADERS.equals(name)) {
				return new TypedValue(((Message<?>) target).getHeaders());
			} else {
				return new TypedValue(((Message<?>) target).getHeaders().get(name));
			}
		}
		throw new AccessException("Unable to read " + target + " using " + name);
	}

	@Override
	public boolean canWrite(EvaluationContext context, Object target, String name) throws AccessException {
		return false;
	}

	@Override
	public void write(EvaluationContext context, Object target, String name, Object newValue) throws AccessException {
		throw new AccessException("Write not supported on default");
	}

}
