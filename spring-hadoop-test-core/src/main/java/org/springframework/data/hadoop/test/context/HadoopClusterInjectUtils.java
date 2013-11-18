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
package org.springframework.data.hadoop.test.context;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.hadoop.test.support.HadoopClusterDelegatingFactoryBean;
import org.springframework.data.hadoop.test.support.ConfigurationDelegatingFactoryBean;
import org.springframework.test.context.MergedContextConfiguration;

/**
 * Utils used in custom context loaders.
 * 
 * @author Janne Valkealahti
 *
 */
abstract class HadoopClusterInjectUtils {

	/**
	 * Create cluster and configuration beans.
	 * 
	 * @param context the context in which the configuration classes should be registered
	 * @param mergedConfig the merged configuration from which the classes should be retrieved
	 */
	public static void handleClusterInject(GenericApplicationContext context,
			MergedContextConfiguration mergedConfig) {
		final Class<MiniHadoopCluster> annotationType = MiniHadoopCluster.class;
		Class<?> testClass = mergedConfig.getTestClass();
		boolean hasMiniHadoopCluster = testClass.isAnnotationPresent(annotationType);
		MiniHadoopCluster annotation = testClass.getAnnotation(annotationType);
		
		if (hasMiniHadoopCluster) {
			String clusterName = annotation.clusterName();
			String configName = annotation.configName();
			String id = annotation.id();
			int nodeCount = annotation.nodes();
			
			BeanDefinitionBuilder builder = BeanDefinitionBuilder
					.genericBeanDefinition(HadoopClusterDelegatingFactoryBean.class);
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
