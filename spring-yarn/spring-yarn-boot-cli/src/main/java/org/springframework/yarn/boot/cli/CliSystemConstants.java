package org.springframework.yarn.boot.cli;

import static java.util.Arrays.asList;

import java.util.List;

public abstract class CliSystemConstants {

	public final static List<String> OPTIONS_APPLICATION_ID  = asList("application-id", "a");

	public final static List<String> OPTIONS_APPLICATION_TYPE  = asList("application-type", "t");

	public final static List<String> OPTIONS_APPLICATION_VERSION  = asList("application-version", "v");

	public final static List<String> OPTIONS_CLUSTER_ID  = asList("cluster-id", "c");

	public final static List<String> OPTIONS_CLUSTER_DEF  = asList("cluster-def", "i");

	public final static List<String> OPTIONS_VERBOSE  = asList("verbose", "v");

	public final static List<String> OPTIONS_PROJECTION_TYPE  = asList("projection-type", "p");

	public final static List<String> OPTIONS_PROJECTION_ANY  = asList("projection-any", "w");

	public final static List<String> OPTIONS_PROJECTION_HOSTS  = asList("projection-hosts", "h");

	public final static List<String> OPTIONS_PROJECTION_RACKS  = asList("projection-racks", "r");

	public final static String DESC_APPLICATION_ID = "Specify YARN application id";

	public final static String DESC_CLUSTER_ID = "Specify cluster id";

	public final static String DESC_CLUSTER_DEF = "Specify cluster def id";

	public final static String DESC_APPLICATION_TYPE = "Application type";

	public final static String DESC_APPLICATION_VERSION = "Application version";

	public final static String DESC_VERBOSE = "Verbose output";

	public final static String DESC_PROJECTION_TYPE = "Projection type";

	public final static String DESC_PROJECTION_ANY = "Projection any count";

	public final static String DESC_PROJECTION_HOSTS = "Projection hosts counts";

	public final static String DESC_PROJECTION_RACKS = "Projection racks counts";

}
