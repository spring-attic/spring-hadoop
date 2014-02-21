package org.springframework.yarn.boot.support;

import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot {@link ConfigurationProperties} for
 * <code>spring.yarn.appmaster</code>.
 *
 * @author Janne Valkealahti
 *
 */
@ConfigurationProperties(name = "spring.yarn.appmaster")
public class SpringYarnAppmasterProperties {

	private String containerFile;
	private String appmasterClass;
	private String containerRunner;
	private boolean waitLatch = true;
	private int containerCount = 1;
	private List<String> classpath;
	private List<String> options;
	private Map<String, String> arguments;
	private Integer priority;
	private String memory;
	private Integer virtualCores;
	private boolean includeSystemEnv = true;
	private boolean defaultYarnAppClasspath = true;
	private boolean includeBaseDirectory = true;
	private String delimiter;
	private List<String> localizerPatterns;
	private String localizerZipPattern;
	private List<String> localizerPropertiesNames;
	private List<String> localizerPropertiesSuffixes;

	public String getContainerFile() {
		return containerFile;
	}

	public void setContainerFile(String containerFile) {
		this.containerFile = containerFile;
	}

	public String getAppmasterClass() {
		return appmasterClass;
	}

	public void setAppmasterClass(String appmasterClass) {
		this.appmasterClass = appmasterClass;
	}

	public String getContainerRunner() {
		return containerRunner;
	}

	public void setContainerRunner(String containerRunner) {
		this.containerRunner = containerRunner;
	}

	public boolean isWaitLatch() {
		return waitLatch;
	}

	public void setWaitLatch(boolean waitLatch) {
		this.waitLatch = waitLatch;
	}

	public int getContainerCount() {
		return containerCount;
	}

	public void setContainerCount(int containerCount) {
		this.containerCount = containerCount;
	}

	public List<String> getClasspath() {
		return classpath;
	}

	public void setClasspath(List<String> classpath) {
		this.classpath = classpath;
	}

	public void setOptions(List<String> options) {
		this.options = options;
	}

	public List<String> getOptions() {
		return options;
	}

	public Map<String, String> getArguments() {
		return arguments;
	}

	public void setArguments(Map<String, String> arguments) {
		this.arguments = arguments;
	}

	public Integer getPriority() {
		return priority;
	}

	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	public String getMemory() {
		return memory;
	}

	public void setMemory(String memory) {
		this.memory = memory;
	}

	public Integer getVirtualCores() {
		return virtualCores;
	}

	public void setVirtualCores(Integer virtualCores) {
		this.virtualCores = virtualCores;
	}

	public boolean isIncludeSystemEnv() {
		return includeSystemEnv;
	}

	public void setIncludeSystemEnv(boolean includeSystemEnv) {
		this.includeSystemEnv = includeSystemEnv;
	}

	public boolean isDefaultYarnAppClasspath() {
		return defaultYarnAppClasspath;
	}

	public void setDefaultYarnAppClasspath(boolean defaultYarnAppClasspath) {
		this.defaultYarnAppClasspath = defaultYarnAppClasspath;
	}

	public boolean isIncludeBaseDirectory() {
		return includeBaseDirectory;
	}

	public void setIncludeBaseDirectory(boolean includeBaseDirectory) {
		this.includeBaseDirectory = includeBaseDirectory;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public List<String> getLocalizerPatterns() {
		return localizerPatterns;
	}

	public void setLocalizerPatterns(List<String> localizerPatterns) {
		this.localizerPatterns = localizerPatterns;
	}

	public String getLocalizerZipPattern() {
		return localizerZipPattern;
	}

	public void setLocalizerZipPattern(String localizerZipPattern) {
		this.localizerZipPattern = localizerZipPattern;
	}

	public List<String> getLocalizerPropertiesNames() {
		return localizerPropertiesNames;
	}

	public void setLocalizerPropertiesNames(List<String> localizerPropertiesNames) {
		this.localizerPropertiesNames = localizerPropertiesNames;
	}

	public List<String> getLocalizerPropertiesSuffixes() {
		return localizerPropertiesSuffixes;
	}

	public void setLocalizerPropertiesSuffixes(List<String> localizerPropertiesSuffixes) {
		this.localizerPropertiesSuffixes = localizerPropertiesSuffixes;
	}

}
