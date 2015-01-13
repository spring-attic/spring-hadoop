/*
 * Copyright 2014-2015 the original author or authors.
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
package org.springframework.yarn.boot.cli;

import static java.util.Arrays.asList;

import java.util.List;

/**
 * Various constants for cli system.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class CliSystemConstants {

	public final static List<String> OPTIONS_APPLICATION_ID  = asList("application-id", "a");

	public final static List<String> OPTIONS_APPLICATION_TYPE  = asList("application-type", "t");

	public final static List<String> OPTIONS_APPLICATION_VERSION  = asList("application-version", "v");

	public final static List<String> OPTIONS_APPLICATION_NAME  = asList("application-name", "n");

	public final static List<String> OPTIONS_CLUSTER_ID  = asList("cluster-id", "c");

	public final static List<String> OPTIONS_CLUSTER_DEF  = asList("cluster-def", "i");

	public final static List<String> OPTIONS_VERBOSE  = asList("verbose", "v");

	public final static List<String> OPTIONS_PROJECTION_TYPE  = asList("projection-type", "p");

	public final static List<String> OPTIONS_PROJECTION_ANY  = asList("projection-any", "w");

	public final static List<String> OPTIONS_PROJECTION_HOSTS  = asList("projection-hosts", "h");

	public final static List<String> OPTIONS_PROJECTION_RACKS  = asList("projection-racks", "r");

	public final static List<String> OPTIONS_PROJECTION_DATA  = asList("projection-data", "y");

	public final static String DESC_APPLICATION_ID = "Specify YARN application id";

	public final static String DESC_CLUSTER_ID = "Specify cluster id";

	public final static String DESC_CLUSTER_DEF = "Specify cluster def id";

	public final static String DESC_APPLICATION_TYPE = "Application type";

	public final static String DESC_APPLICATION_VERSION = "Application version";

	public final static String DESC_APPLICATION_NAME = "Application name";

	public final static String DESC_VERBOSE = "Verbose output";

	public final static String DESC_PROJECTION_TYPE = "Projection type";

	public final static String DESC_PROJECTION_ANY = "Projection any count";

	public final static String DESC_PROJECTION_HOSTS = "Projection hosts counts";

	public final static String DESC_PROJECTION_RACKS = "Projection racks counts";

	public final static String DESC_PROJECTION_DATA = "Raw projection data";

}
