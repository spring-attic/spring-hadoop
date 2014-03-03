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

package org.springframework.yarn.test.context;

import java.lang.annotation.Annotation;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.style.ToStringCreator;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import static org.springframework.core.annotation.AnnotationUtils.*;

/**
 * {@code MetaAnnotationUtils} is a collection of utility methods that complements
 * support already available in {@link AnnotationUtils}.
 *
 * <p>Whereas {@code AnnotationUtils} only provides utilities for <em>getting</em>
 * or <em>finding</em> an annotation, {@code MetaAnnotationUtils} provides
 * additional support for determining the <em>root class</em> on which an
 * annotation is declared, either directly or via a <em>composed annotation</em>.
 * This additional information is encapsulated in an {@link AnnotationDescriptor}.
 *
 * <p>The additional information provided by an {@code AnnotationDescriptor} is
 * required in the <em>Spring TestContext Framework</em> in order to be able to
 * support class hierarchy traversals for <em>inherited</em> annotations such as
 * {@link ContextConfiguration @ContextConfiguration},
 * {@link TestExecutionListeners @TestExecutionListeners}, and
 * {@link ActiveProfiles @ActiveProfiles}.
 *
 * <p>NOTE: copied from spring-test due to it being package scoped
 *
 * @author Sam Brannen
 * @author Janne Valkealahti
 * @see AnnotationUtils
 * @see AnnotationDescriptor
 */
abstract class MetaAnnotationUtils {

	private MetaAnnotationUtils() {
		/* no-op */
	}

	/**
	 * Find the {@link AnnotationDescriptor} for the supplied {@code annotationType}
	 * from the supplied {@link Class}, traversing its annotations and superclasses
	 * if no annotation can be found on the given class itself.
	 *
	 * <p>This method explicitly handles class-level annotations which are not
	 * declared as {@linkplain java.lang.annotation.Inherited inherited} <em>as
	 * well as meta-annotations</em>.
	 *
	 * <p>The algorithm operates as follows:
	 * <ol>
	 *   <li>Search for a local declaration of the annotation on the given class
	 *   and return a corresponding {@code AnnotationDescriptor} if found.
	 *   <li>Search through all annotations that the given class declares,
	 *   returning an {@code AnnotationDescriptor} for the first matching
	 *   candidate, if any.
	 *   <li>Proceed with introspection of the superclass hierarchy of the given
	 *   class by returning to step #1 with the superclass as the class to look
	 *   for annotations on.
	 * </ol>
	 *
	 * <p>If the supplied {@code clazz} is an interface, only the interface
	 * itself will be checked; the inheritance hierarchy for interfaces will not
	 * be traversed.
	 *
	 * @param clazz the class to look for annotations on
	 * @param annotationType the annotation class to look for, both locally and
	 * as a meta-annotation
	 * @return the corresponding annotation descriptor if the annotation was found;
	 * otherwise {@code null}
	 * @see AnnotationUtils#findAnnotationDeclaringClass(Class, Class)
	 * @see #findAnnotationDescriptorForTypes(Class, Class...)
	 */
	public static <T extends Annotation> AnnotationDescriptor<T> findAnnotationDescriptor(Class<?> clazz,
			Class<T> annotationType) {

		Assert.notNull(annotationType, "Annotation type must not be null");

		if (clazz == null || clazz.equals(Object.class)) {
			return null;
		}

		// Declared locally?
		if (isAnnotationDeclaredLocally(annotationType, clazz)) {
			return new AnnotationDescriptor<T>(clazz, clazz.getAnnotation(annotationType));
		}

		// Declared on a composed annotation (i.e., as a meta-annotation)?
		if (!Annotation.class.isAssignableFrom(clazz)) {
			for (Annotation composedAnnotation : clazz.getAnnotations()) {
				T annotation = composedAnnotation.annotationType().getAnnotation(annotationType);
				if (annotation != null) {
					return new AnnotationDescriptor<T>(clazz, composedAnnotation, annotation);
				}
			}
		}

		// Declared on a superclass?
		return findAnnotationDescriptor(clazz.getSuperclass(), annotationType);
	}

	/**
	 * Find the {@link UntypedAnnotationDescriptor} for the first {@link Class}
	 * in the inheritance hierarchy of the specified {@code clazz} (including
	 * the specified {@code clazz} itself) which declares at least one of the
	 * specified {@code annotationTypes}, or {@code null} if none of the
	 * specified annotation types could be found.
	 *
	 * <p>This method traverses the annotations and superclasses of the specified
	 * {@code clazz} if no annotation can be found on the given class itself.
	 *
	 * <p>This method explicitly handles class-level annotations which are not
	 * declared as {@linkplain java.lang.annotation.Inherited inherited} <em>as
	 * well as meta-annotations</em>.
	 *
	 * <p>The algorithm operates as follows:
	 * <ol>
	 *   <li>Search for a local declaration of one of the annotation types on
	 *   the given class and return a corresponding {@code UntypedAnnotationDescriptor}
	 *   if found.
	 *   <li>Search through all annotations that the given class declares,
	 *   returning an {@code UntypedAnnotationDescriptor} for the first matching
	 *   candidate, if any.
	 *   <li>Proceed with introspection of the superclass hierarchy of the given
	 *   class by returning to step #1 with the superclass as the class to look
	 *   for annotations on.
	 * </ol>
	 *
	 * <p>If the supplied {@code clazz} is an interface, only the interface
	 * itself will be checked; the inheritance hierarchy for interfaces will not
	 * be traversed.
	 *
	 * @param clazz the class to look for annotations on
	 * @param annotationTypes the types of annotations to look for, both locally
	 * and as meta-annotations
	 * @return the corresponding annotation descriptor if one of the annotations
	 * was found; otherwise {@code null}
	 * @see AnnotationUtils#findAnnotationDeclaringClassForTypes(java.util.List, Class)
	 * @see #findAnnotationDescriptor(Class, Class)
	 */
	public static UntypedAnnotationDescriptor findAnnotationDescriptorForTypes(Class<?> clazz,
			Class<? extends Annotation>... annotationTypes) {

		assertNonEmptyAnnotationTypeArray(annotationTypes, "The list of annotation types must not be empty");

		if (clazz == null || clazz.equals(Object.class)) {
			return null;
		}

		// Declared locally?
		for (Class<? extends Annotation> annotationType : annotationTypes) {
			if (isAnnotationDeclaredLocally(annotationType, clazz)) {
				return new UntypedAnnotationDescriptor(clazz, clazz.getAnnotation(annotationType));
			}
		}

		// Declared on a composed annotation (i.e., as a meta-annotation)?
		if (!Annotation.class.isAssignableFrom(clazz)) {
			for (Annotation composedAnnotation : clazz.getAnnotations()) {
				for (Class<? extends Annotation> annotationType : annotationTypes) {
					Annotation annotation = composedAnnotation.annotationType().getAnnotation(annotationType);
					if (annotation != null) {
						return new UntypedAnnotationDescriptor(clazz, composedAnnotation, annotation);
					}
				}
			}
		}

		// Declared on a superclass?
		return findAnnotationDescriptorForTypes(clazz.getSuperclass(), annotationTypes);
	}


	/**
	 * Descriptor for an {@link Annotation}, including the {@linkplain
	 * #getDeclaringClass() class} on which the annotation is <em>declared</em>
	 * as well as the actual {@linkplain #getAnnotation() annotation} instance.
	 *
	 * <p>
	 * If the annotation is used as a meta-annotation, the descriptor also includes
	 * the {@linkplain #getComposedAnnotation() composed annotation} on which the
	 * annotation is present. In such cases, the <em>root declaring class</em> is
	 * not directly annotated with the annotation but rather indirectly via the
	 * composed annotation.
	 *
	 * <p>
	 * Given the following example, if we are searching for the {@code @Transactional}
	 * annotation <em>on</em> the {@code TransactionalTests} class, then the
	 * properties of the {@code AnnotationDescriptor} would be as follows.
	 *
	 * <ul>
	 *   <li>rootDeclaringClass: {@code TransactionalTests} class object</li>
	 *   <li>declaringClass: {@code TransactionalTests} class object</li>
	 *   <li>composedAnnotation: {@code null}</li>
	 *   <li>annotation: instance of the {@code Transactional} annotation</li>
	 * </ul>
	 *
	 * <pre style="code">
	 * &#064;Transactional
	 * &#064;ContextConfiguration({"/test-datasource.xml", "/repository-config.xml"})
	 * public class TransactionalTests { }
	 * </pre>
	 *
	 * <p>
	 * Given the following example, if we are searching for the {@code @Transactional}
	 * annotation <em>on</em> the {@code UserRepositoryTests} class, then the
	 * properties of the {@code AnnotationDescriptor} would be as follows.
	 *
	 * <ul>
	 *   <li>rootDeclaringClass: {@code UserRepositoryTests} class object</li>
	 *   <li>declaringClass: {@code RepositoryTests} class object</li>
	 *   <li>composedAnnotation: instance of the {@code RepositoryTests} annotation</li>
	 *   <li>annotation: instance of the {@code Transactional} annotation</li>
	 * </ul>
	 *
	 * <pre style="code">
	 * &#064;Transactional
	 * &#064;ContextConfiguration({"/test-datasource.xml", "/repository-config.xml"})
	 * &#064;Retention(RetentionPolicy.RUNTIME)
	 * public &#064;interface RepositoryTests { }
	 *
	 * &#064;RepositoryTests
	 * public class UserRepositoryTests { }
	 * </pre>
	 *
	 * @author Sam Brannen
	 */
	public static class AnnotationDescriptor<T extends Annotation> {

		private final Class<?> rootDeclaringClass;
		private final Class<?> declaringClass;
		private final Annotation composedAnnotation;
		private final T annotation;
		private final AnnotationAttributes annotationAttributes;


		public AnnotationDescriptor(Class<?> rootDeclaringClass, T annotation) {
			this(rootDeclaringClass, null, annotation);
		}

		public AnnotationDescriptor(Class<?> rootDeclaringClass, Annotation composedAnnotation, T annotation) {
			Assert.notNull(rootDeclaringClass, "rootDeclaringClass must not be null");
			Assert.notNull(annotation, "annotation must not be null");

			this.rootDeclaringClass = rootDeclaringClass;
			this.declaringClass = (composedAnnotation != null) ? composedAnnotation.annotationType()
					: rootDeclaringClass;
			this.composedAnnotation = composedAnnotation;
			this.annotation = annotation;
			this.annotationAttributes = AnnotatedElementUtils.getAnnotationAttributes(rootDeclaringClass,
				annotation.annotationType().getName());
		}

		public Class<?> getRootDeclaringClass() {
			return this.rootDeclaringClass;
		}

		public Class<?> getDeclaringClass() {
			return this.declaringClass;
		}

		public T getAnnotation() {
			return this.annotation;
		}

		public Class<? extends Annotation> getAnnotationType() {
			return this.annotation.annotationType();
		}

		public AnnotationAttributes getAnnotationAttributes() {
			return this.annotationAttributes;
		}

		public Annotation getComposedAnnotation() {
			return this.composedAnnotation;
		}

		public Class<? extends Annotation> getComposedAnnotationType() {
			return this.composedAnnotation == null ? null : this.composedAnnotation.annotationType();
		}

		/**
		 * Provide a textual representation of this {@code AnnotationDescriptor}.
		 */
		@Override
		public String toString() {
			return new ToStringCreator(this)//
			.append("rootDeclaringClass", rootDeclaringClass)//
			.append("declaringClass", declaringClass)//
			.append("composedAnnotation", composedAnnotation)//
			.append("annotation", annotation)//
			.toString();
		}
	}

	/**
	 * <em>Untyped</em> extension of {@code AnnotationDescriptor} that is used
	 * to describe the declaration of one of several candidate annotation types
	 * where the actual annotation type cannot be predetermined.
	 *
	 * @author Sam Brannen
	 */
	public static class UntypedAnnotationDescriptor extends AnnotationDescriptor<Annotation> {

		public UntypedAnnotationDescriptor(Class<?> declaringClass, Annotation annotation) {
			super(declaringClass, annotation);
		}

		public UntypedAnnotationDescriptor(Class<?> declaringClass, Annotation composedAnnotation, Annotation annotation) {
			super(declaringClass, composedAnnotation, annotation);
		}
	}


	private static void assertNonEmptyAnnotationTypeArray(Class<?>[] annotationTypes, String message) {
		if (ObjectUtils.isEmpty(annotationTypes)) {
			throw new IllegalArgumentException(message);
		}

		for (Class<?> clazz : annotationTypes) {
			if (!Annotation.class.isAssignableFrom(clazz)) {
				throw new IllegalArgumentException("Array elements must be of type Annotation");
			}
		}
	}

}
