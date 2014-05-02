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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.yarn.test.YarnTestSystemConstants;

/**
 * {@code MiniYarnCluster} defines class-level metadata that is
 * used to determine how to load and configure a mini cluster
 * and inject it into {@link org.springframework.context.ApplicationContext ApplicationContext}
 * for test classes.
 * 
 * @author Janne Valkealahti
 *
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface MiniYarnCluster {

	/**
	 * Value defining a name used to set
	 * {@link org.apache.hadoop.conf.Configuration}
	 * bean based on mini cluster runtime config.
	 * Default is "yarnConfiguration"
	 *
	 * @return config name
	 */
	String configName() default YarnTestSystemConstants.DEFAULT_ID_MINIYARNCLUSTER_CONFIG;
	
	/**
	 * Value defining a name used to set the
	 * cluster bean. Default is "yarnCluster"
	 *
	 * @return cluster name
	 */
	String clusterName() default YarnTestSystemConstants.DEFAULT_ID_MINIYARNCLUSTER;
	
	/**
	 * Unique id for the cluster. Default
	 * is "default".
	 *
	 * @return id
	 */
	String id() default YarnTestSystemConstants.DEFAULT_ID_CLUSTER;
	
	/**
	 * Number of nodes for the cluster.
	 * Default size is one node.
	 *
	 * @return number of nodes
	 */
	int nodes() default 1;

}
