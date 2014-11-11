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

import org.springframework.asm.MethodVisitor;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.CodeFlow;
import org.springframework.expression.spel.CompilablePropertyAccessor;
import org.springframework.expression.spel.support.ReflectivePropertyAccessor;
import org.springframework.messaging.Message;

/**
 * A {@link PropertyAccessor} reading values from a backing {@link Message} used by a
 * partition key. In a way this is similar than {@link MapAccessor} for header but also
 * adds 'payload' and 'headers' to be resolved. Having 'payload' or 'headers' keywords
 * in headers is not possible to access via this accessor.
 *
 * @author Janne Valkealahti
 *
 */
public class MessagePartitionKeyPropertyAccessor extends ReflectivePropertyAccessor {

	private final static Class<?>[] CLASSES = new Class<?>[] { Message.class };

	private final static String PAYLOAD = "payload";

	private final static String HEADERS = "headers";

	private final static String TIMESTAMP = "timestamp";

	private final static String messageClassDescriptor = "org/springframework/messaging/Message";

	private final static String messageHeadersClassDescriptor = "org/springframework/messaging/MessageHeaders";

	private volatile String typeDescriptor;

	@Override
	public Class<?>[] getSpecificTargetClasses() {
		return CLASSES;
	}

	@Override
	public boolean canRead(EvaluationContext context, Object target, String name) throws AccessException {
		if (target instanceof Message && !PAYLOAD.equals(name) && !HEADERS.equals(name)) {
			boolean containsKey = ((Message<?>) target).getHeaders().containsKey(name);
			if (containsKey) {
				this.typeDescriptor = CodeFlow.toDescriptorFromObject(((Message<?>) target).getHeaders().get(name));
			}
			return containsKey;
		}
		return super.canRead(context, target, name);
	}

	@Override
	public TypedValue read(EvaluationContext context, Object target, String name) throws AccessException {
		if (target instanceof Message) {
			if (PAYLOAD.equals(name) || HEADERS.equals(name)) {
				return super.read(context, target, name);
			} else {
				return super.read(context, ((Message<?>)target).getHeaders(), name);
			}
		}
		return super.read(context, target, name);
	}

	@Override
	public boolean canWrite(EvaluationContext context, Object target, String name) throws AccessException {
		return super.canWrite(context, target, name);
	}

	@Override
	public void write(EvaluationContext context, Object target, String name, Object newValue) throws AccessException {
		super.write(context, target, name, newValue);
	}

	@Override
	public PropertyAccessor createOptimalAccessor(EvaluationContext evalContext, Object target, String name) {
		if (target instanceof Message && !PAYLOAD.equals(name) && !HEADERS.equals(name)) {
			return new MessageOptimalPropertyAccessor(this.typeDescriptor);
		}
		return super.createOptimalAccessor(evalContext, target, name);
	}

	public static class MessageOptimalPropertyAccessor implements CompilablePropertyAccessor {

		private final String typeDescriptor;

		public MessageOptimalPropertyAccessor(String typeDescriptor) {
			this.typeDescriptor = typeDescriptor;
		}

		@Override
		public Class<?>[] getSpecificTargetClasses() {
			throw new UnsupportedOperationException("Should not be called on an MessageOptimalPropertyAccessor");
		}

		@Override
		public boolean canRead(EvaluationContext context, Object target, String name) throws AccessException {
			return true;
		}

		@Override
		public TypedValue read(EvaluationContext context, Object target, String name) throws AccessException {
			return new TypedValue(((Message<?>) target).getHeaders().get(name));
		}

		@Override
		public boolean canWrite(EvaluationContext context, Object target, String name) throws AccessException {
			throw new UnsupportedOperationException("Should not be called on an MessageOptimalPropertyAccessor");
		}

		@Override
		public void write(EvaluationContext context, Object target, String name, Object newValue)
				throws AccessException {
			throw new UnsupportedOperationException("Should not be called on an MessageOptimalPropertyAccessor");
		}

		@Override
		public boolean isCompilable() {
			return true;
		}

		@Override
		public Class<?> getPropertyType() {
			return Object.class;
		}

		@Override
		public void generateCode(String propertyName, MethodVisitor mv, CodeFlow cf) {
			String descriptor = cf.lastDescriptor();

			if (descriptor == null) {
				cf.loadTarget(mv);
			}
			if (descriptor == null || !messageClassDescriptor.equals(descriptor.substring(1))) {
				mv.visitTypeInsn(CHECKCAST, messageClassDescriptor);
			}

			mv.visitMethodInsn(INVOKEINTERFACE, messageClassDescriptor, "getHeaders",
					"()L" + messageHeadersClassDescriptor + ";", true);

			if (TIMESTAMP.equals(propertyName)) {
				mv.visitMethodInsn(INVOKEVIRTUAL, messageHeadersClassDescriptor, "getTimestamp",
						"()Ljava/lang/Long;", false);
			} else {
				mv.visitLdcInsn(propertyName);
				mv.visitMethodInsn(INVOKEVIRTUAL, messageHeadersClassDescriptor, "get",
						"(Ljava/lang/Object;)Ljava/lang/Object;", false);
			}

			if (typeDescriptor != null) {
				CodeFlow.insertCheckCast(mv, typeDescriptor);
			}

		}

	}

}
