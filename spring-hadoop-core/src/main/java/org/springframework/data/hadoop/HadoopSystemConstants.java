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
package org.springframework.data.hadoop;

/**
 * Various constants throughout the Spring Hadoop libraries.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class HadoopSystemConstants {

	/** Default bean id for resource loader. */
	public static final String DEFAULT_ID_RESOURCE_LOADER = "hadoopResourceLoader";

	/** Default bean id for resource loader registrar. */
	public static final String DEFAULT_ID_RESOURCE_LOADER_REGISTRAR = "hadoopResourceLoaderRegistrar";

	/** Default bean id for hadoop configuration. */
	public static final String DEFAULT_ID_CONFIGURATION = "hadoopConfiguration";

	/** Default bean id for hadoop fsshell. */
	public static final String DEFAULT_ID_FSSHELL = "hadoopFsShell";

	/** Default env variable for resource manager address. */
	public static final String RM_ADDRESS = "SHDP_HD_RM";

	/** Default env variable for hdfs address. */
	public static final String FS_ADDRESS = "SHDP_HD_FS";

	/** Default env variable for resource manager scheduler address. */
	public static final String SCHEDULER_ADDRESS = "SHDP_HD_SCHEDULER";
	
}
