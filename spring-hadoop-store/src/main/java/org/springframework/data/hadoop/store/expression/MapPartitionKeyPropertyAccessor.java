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

import java.util.Map;

import org.springframework.asm.MethodVisitor;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.CodeFlow;
import org.springframework.expression.spel.CompilablePropertyAccessor;
import org.springframework.expression.spel.support.ReflectivePropertyAccessor;

/**
 * A {@link PropertyAccessor} reading values from a backing {@link Map} used by a
 * partition key.
 *
 * @author Janne Valkealahti
 *
 */
public class MapPartitionKeyPropertyAccessor extends ReflectivePropertyAccessor {

	private final static Class<?>[] CLASSES = new Class<?>[] { Map.class };

	private final static String mapClassDescriptor = "java/util/Map";

	private volatile String typeDescriptor;

	@Override
	public Class<?>[] getSpecificTargetClasses() {
		return CLASSES;
	}

	@Override
	public boolean canRead(EvaluationContext context, Object target, String name) throws AccessException {
		boolean containsKey = ((Map<?, ?>) target).containsKey(name);
		if (containsKey) {
			this.typeDescriptor = CodeFlow.toDescriptorFromObject(((Map<?, ?>) target).get(name));
		}
		return containsKey;
	}

	@Override
	public TypedValue read(EvaluationContext context, Object target, String name) throws AccessException {
		Map<?, ?> map = (Map<?, ?>) target;
		Object value = map.get(name);
		if (value == null && !map.containsKey(name)) {
			throw new MapAccessException(name);
		}
		return new TypedValue(value);
	}

	@Override
	public PropertyAccessor createOptimalAccessor(EvaluationContext evalContext, Object target, String name) {
		return new MapOptimalPropertyAccessor(this.typeDescriptor);
	}

	public static class MapOptimalPropertyAccessor implements CompilablePropertyAccessor {

		private final String typeDescriptor;

		public MapOptimalPropertyAccessor(String typeDescriptor) {
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
			return new TypedValue(((Map<?,?>) target).get(name));
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
			if (descriptor == null || !mapClassDescriptor.equals(descriptor.substring(1))) {
				mv.visitTypeInsn(CHECKCAST, mapClassDescriptor);
			}

			mv.visitLdcInsn(propertyName);
			mv.visitMethodInsn(INVOKEINTERFACE, mapClassDescriptor, "get",
					"(Ljava/lang/Object;)Ljava/lang/Object;", true);

			if (typeDescriptor != null) {
				CodeFlow.insertCheckCast(mv, typeDescriptor);
			}
		}

	}

	/**
	 * Exception thrown from {@code read} in order to reset a cached
	 * PropertyAccessor, allowing other accessors to have a try.
	 */
	@SuppressWarnings("serial")
	private static class MapAccessException extends AccessException {

		private final String key;

		public MapAccessException(String key) {
			super(null);
			this.key = key;
		}

		@Override
		public String getMessage() {
			return "Map does not contain a value for key '" + this.key + "'";
		}
	}

}
