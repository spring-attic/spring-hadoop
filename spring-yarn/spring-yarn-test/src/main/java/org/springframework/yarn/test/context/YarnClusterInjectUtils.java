/*
 * Copyright 2013 the original author or authors.
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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.context.MergedContextConfiguration;
import org.springframework.yarn.test.context.MetaAnnotationUtils.AnnotationDescriptor;
import org.springframework.yarn.test.support.ClusterDelegatingFactoryBean;
import org.springframework.yarn.test.support.ConfigurationDelegatingFactoryBean;

/**
 * Utils used in custom context loaders.
 *
 * @author Janne Valkealahti
 *
 */
abstract class YarnClusterInjectUtils {

	/**
	 * Create cluster and configuration beans.
	 *
	 * @param context the context in which the configuration classes should be registered
	 * @param mergedConfig the merged configuration from which the classes should be retrieved
	 */
	public static void handleClusterInject(GenericApplicationContext context,
			MergedContextConfiguration mergedConfig) {
		Class<?> testClass = mergedConfig.getTestClass();
		AnnotationDescriptor<MiniYarnCluster> annotationDescriptor = MetaAnnotationUtils.findAnnotationDescriptor(
				testClass, MiniYarnCluster.class);

		if (annotationDescriptor != null && annotationDescriptor.getAnnotation() != null) {
			String clusterName = annotationDescriptor.getAnnotationAttributes().getString("clusterName");
			String configName = annotationDescriptor.getAnnotationAttributes().getString("configName");
			String id = annotationDescriptor.getAnnotationAttributes().getString("id");
			int nodeCount = annotationDescriptor.getAnnotationAttributes().getNumber("nodes").intValue();

			BeanDefinitionBuilder builder = BeanDefinitionBuilder
					.genericBeanDefinition(ClusterDelegatingFactoryBean.class);
			builder.addPropertyValue("id", id);
			builder.addPropertyValue("nodes", nodeCount);
			context.registerBeanDefinition(clusterName, builder.getBeanDefinition());

			builder = BeanDefinitionBuilder
					.genericBeanDefinition(ConfigurationDelegatingFactoryBean.class);
			builder.addPropertyReference("cluster", clusterName);
			context.registerBeanDefinition(configName, builder.getBeanDefinition());
		}

	}

}
