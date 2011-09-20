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
package org.springframework.data.hadoop.util.reflect;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.aop.support.AopUtils;
import org.springframework.core.LocalVariableTableParameterNameDiscoverer;
import org.springframework.core.MethodParameter;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.MethodCallback;
import org.springframework.util.ReflectionUtils.MethodFilter;

/**
 * @author Dave Syer
 * 
 */
public abstract class MethodUtils {

	public static Method getSingleNamedMethod(Class<?> targetClass, final String methodName)
			throws IllegalArgumentException {
		final List<Method> list = new ArrayList<Method>();
		ReflectionUtils.doWithMethods(targetClass, new MethodCallback() {
			public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
				if (method.getName().equals(methodName)) {
					list.add(method);
				}
			}
		}, new UniqueMethodFilter(targetClass));
		if (list.size() != 1) {
			throw new IllegalArgumentException("No single method called " + methodName + " found in "
					+ targetClass.getName());
		}
		return list.get(0);
	}

	public static Method getSingleAnnotatedOrPublicMethod(Class<?> targetClass,
			final Class<? extends Annotation> annotationType) throws IllegalArgumentException {
		List<Method> annotatedMethods = getAnnotatedMethods(targetClass, annotationType);
		if (annotatedMethods.size() == 1) {
			return annotatedMethods.get(0);
		}
		if (annotatedMethods.size() > 1) {
			throw new IllegalArgumentException("More than one method annotated with " + annotationType.getSimpleName());
		}
		if (annotatedMethods.isEmpty()) {
			List<Method> publicMethods = getUniquePublicMethods(targetClass);
			if (publicMethods.size() == 1) {
				return publicMethods.get(0);
			}
			if (publicMethods.size() > 1) {
				throw new IllegalArgumentException("More than one public method and none annotated with "
						+ annotationType.getSimpleName());
			}
		}
		throw new IllegalArgumentException("No public methods in " + targetClass.getName());
	}

	public static List<Method> getAnnotatedMethods(Class<?> targetClass,
			final Class<? extends Annotation> annotationType) {
		final List<Method> list = new ArrayList<Method>();
		ReflectionUtils.doWithMethods(targetClass, new MethodCallback() {
			public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
				if (AnnotationUtils.findAnnotation(method, annotationType) != null) {
					list.add(method);
				}
			}
		}, new UniqueMethodFilter(targetClass));
		return list;
	}

	public static List<Method> getUniquePublicMethods(Class<?> targetClass) {
		final List<Method> list = new ArrayList<Method>();
		ReflectionUtils.doWithMethods(targetClass, new MethodCallback() {
			public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
				list.add(method);
			}
		}, new UniqueMethodFilter(targetClass));
		return list;
	}
	
	public static List<MethodParameter> getParameterTypes(Method method) {
		return getParameterTypes(method, new LocalVariableTableParameterNameDiscoverer());
	}
	
	public static List<MethodParameter> getParameterTypes(Method method, ParameterNameDiscoverer parameterNameDiscoverer) {
			
		Class<?>[] paramTypes = method.getParameterTypes();
		Object[] args = new Object[paramTypes.length];
		List<MethodParameter> parameters = new ArrayList<MethodParameter>();

		for (int i = 0; i < args.length; i++) {
			MethodParameter parameter = new MethodParameter(method, i);
			parameter.initParameterNameDiscovery(parameterNameDiscoverer);
			parameters.add(parameter);
		}
		
		return parameters;
		
	}

	private static class UniqueMethodFilter implements MethodFilter {

		private final List<Method> uniqueMethods = new ArrayList<Method>();

		public UniqueMethodFilter(Class<?> targetClass) {
			ArrayList<Method> allMethods = new ArrayList<Method>(Arrays.asList(ReflectionUtils
					.getAllDeclaredMethods(targetClass)));
			for (Method method : allMethods) {
				this.uniqueMethods.add(ClassUtils.getMostSpecificMethod(method, targetClass));
			}
		}

		public boolean matches(Method method) {
			if (method.isBridge()) {
				return false;
			}
			if (!Modifier.isPublic(method.getModifiers())) {
				return false;
			}
			if (isMethodDefinedOnObjectClass(method)) {
				return false;
			}
			return this.uniqueMethods.contains(method);
		}

		private boolean isMethodDefinedOnObjectClass(Method method) {
			if (method == null) {
				return false;
			}
			if (method.getDeclaringClass().equals(Object.class)) {
				return true;
			}
			if (ReflectionUtils.isEqualsMethod(method) || ReflectionUtils.isHashCodeMethod(method)
					|| ReflectionUtils.isToStringMethod(method) || AopUtils.isFinalizeMethod(method)) {
				return true;
			}
			return (method.getName().equals("clone") && method.getParameterTypes().length == 0);
		}

	}

	public static Class<?> getTargetClass(Object targetObject) {
		Class<?> targetClass = targetObject.getClass();
		if (AopUtils.isAopProxy(targetObject)) {
			targetClass = AopUtils.getTargetClass(targetObject);
		}
		else if (AopUtils.isCglibProxyClass(targetClass)) {
			Class<?> superClass = targetObject.getClass().getSuperclass();
			if (!Object.class.equals(superClass)) {
				targetClass = superClass;
			}
		}
		return targetClass;
	}
}
