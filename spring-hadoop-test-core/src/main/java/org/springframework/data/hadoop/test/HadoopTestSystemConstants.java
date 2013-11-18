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
package org.springframework.data.hadoop.test;

/**
 * Various constants throughout the Spring Hadoop testing libraries.
 *
 * @author Janne Valkealahti
 *
 */
public class HadoopTestSystemConstants {

	/** Base hdfs path for tests. */
	public static final String HDFS_TESTS_BASE_PATH = "/shdp/";

	/** Default bean id for mini hadoop cluster. */
	public static final String DEFAULT_ID_MINIHADOOPCLUSTER = "hadoopCluster";

	/** Default bean id for mini hadoop cluster configured configuration. */
	public static final String DEFAULT_ID_MINIHADOOPCLUSTER_CONFIG = "hadoopConfiguration";

	/** Default id for mini hadoop cluster. */
	public static final String DEFAULT_ID_CLUSTER = "default";

}
