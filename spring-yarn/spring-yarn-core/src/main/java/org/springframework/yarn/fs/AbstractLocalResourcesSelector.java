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
package org.springframework.yarn.fs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PatternMatchUtils;
import org.springframework.util.StringUtils;

/**
 * Base implementation for {@link LocalResourcesSelector} supporting simple
 * use cases where user needs to localise properties, zip and jar files.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractLocalResourcesSelector implements LocalResourcesSelector {

	/** Default pattern matching zip archives */
	private final static String DEFAULT_PATTERN_ZIP_ARCHIVE = "*zip";

	/** Default array of property file base names */
	private final static String[] DEFAULT_PROPERTIES_NAMES = new String[]{"application"};

	/** Default array of property file suffixes */
	private final static String[] DEFAULT_PROPERTIES_SUFFIXES = new String[]{"properties", "yml"};

	/** Array of base property file names */
	private String[] propertiesNames = DEFAULT_PROPERTIES_NAMES;

	/** Array of property file suffixes */
	private String[] propertiesSuffixes = DEFAULT_PROPERTIES_SUFFIXES;

	/** Pattern used to match zip archives */
	private String zipArchivePattern = DEFAULT_PATTERN_ZIP_ARCHIVE;

	/** Additional selector patterns */
	private final List<String> patterns = new ArrayList<String>();

	@Override
	public final List<Entry> select(String dir) {
		return selectInternal(dir);
	}

	/**
	 * Adds a pattern to be returned selected. Empty
	 * pattern is not added.
	 *
	 * @param pattern the pattern
	 */
	public void addPattern(String pattern) {
		if (StringUtils.hasText(pattern)) {
			patterns.add(pattern);
		}
	}

	/**
	 * Adds a patterns to be returned as selected.
	 *
	 * @param patterns the patterns
	 * @see #addPattern(String)
	 */
	public void addPatterns(String... patterns) {
		if (!ObjectUtils.isEmpty(patterns)) {
			addPatterns(Arrays.asList(patterns));
		}
	}

	/**
	 * Adds a patterns to be returned as selected.
	 *
	 * @param patterns the patterns
	 * @see #addPattern(String)
	 */
	public void addPatterns(List<String> patterns) {
		if (patterns != null) {
			this.patterns.addAll(patterns);
		}
	}

	/**
	 * Gets the patterns.
	 *
	 * @return the patterns
	 */
	public List<String> getPatterns() {
		return patterns;
	}

	/**
	 * Sets the zip archive pattern. Default pattern defined
	 * as {@link #DEFAULT_PATTERN_ZIP_ARCHIVE}. Setting this
	 * pattern as <code>NULL</code> effectively disables
	 * zip matching.
	 *
	 * @param zipArchivePattern the new zip archive pattern
	 */
	public void setZipArchivePattern(String zipArchivePattern) {
		this.zipArchivePattern = zipArchivePattern;
	}

	/**
	 * Gets the current zip archive pattern.
	 *
	 * @return the zip archive pattern
	 */
	public String getZipArchivePattern() {
		return zipArchivePattern;
	}

	/**
	 * Sets the properties names. These will be used
	 * as property file names together with values
	 * returned from {@link #getPropertiesSuffixes()}.
	 *
	 * @param propertiesNames the new properties names
	 */
	public void setPropertiesNames(String... propertiesNames) {
		this.propertiesNames = propertiesNames;
	}

	/**
	 * Sets the properties names.
	 *
	 * @param propertiesNames the new properties names
	 * @see #setPropertiesNames(String...)
	 */
	public void setPropertiesNames(List<String> propertiesNames) {
		setPropertiesNames(StringUtils.toStringArray(propertiesNames));
	}

	/**
	 * Gets the properties fiels base names.
	 *
	 * @return the properties names
	 */
	public String[] getPropertiesNames() {
		return propertiesNames;
	}

	/**
	 * Sets the properties suffixes. These will be used
	 * as property file names together with values
	 * returned from {@link #getPropertiesNames()}.
	 *
	 * @param propertiesSuffixes the new properties suffixes
	 */
	public void setPropertiesSuffixes(String... propertiesSuffixes) {
		this.propertiesSuffixes = propertiesSuffixes;
	}

	/**
	 * Sets the properties suffixes.
	 *
	 * @param propertiesSuffixes the new properties suffixes
	 * @see #setPropertiesSuffixes(String...)
	 */
	public void setPropertiesSuffixes(List<String> propertiesSuffixes) {
		setPropertiesSuffixes(StringUtils.toStringArray(propertiesSuffixes));
	}

	/**
	 * Gets the properties files suffixes.
	 *
	 * @return the properties suffixes
	 */
	public String[] getPropertiesSuffixes() {
		return propertiesSuffixes;
	}

	/**
	 * Select internal.
	 *
	 * @param dir the dir
	 * @return the list
	 * @see #select(String)
	 */
	protected List<Entry> selectInternal(String dir) {
		List<Entry> entries = new ArrayList<Entry>();
		for (String name : createNamesList(getPropertiesNames(), getPropertiesSuffixes())) {
			entries.add(new Entry(dir + name, null));
		}
		for (String pattern : getPatterns()) {
			if (StringUtils.hasText(pattern)) {
				if (pattern.startsWith("/")) {
					// full root path, don't add dir prefix
					entries.add(new Entry(pattern, isZipArchive(pattern) ? LocalResourceType.ARCHIVE : null));
				} else {
					entries.add(new Entry(dir + pattern, isZipArchive(pattern) ? LocalResourceType.ARCHIVE : null));
				}
			}
		}
		return entries;
	}

	/**
	 * Matching if argument is determined to be a zip archive.
	 * Uses {@link PatternMatchUtils#simpleMatch(String, String)}
	 * for actual matching. Match pattern is defined in variable
	 * {@link #zipArchivePattern} and can be altered using method
	 * {@link #setZipArchivePattern(String)}.
	 *
	 * @param name the name to match
	 * @return true, if is matched as zip archive
	 */
	protected boolean isZipArchive(String name) {
		return PatternMatchUtils.simpleMatch(zipArchivePattern, name);
	}

	private static List<String> createNamesList(String[] bases, String[] suffixes) {
		List<String> names = new ArrayList<String>();
		if (!ObjectUtils.isEmpty(bases) && !ObjectUtils.isEmpty(suffixes)) {
			for (String base : bases) {
				for (String suffix : suffixes) {
					names.add(base + "." + suffix);
				}
			}
		}
		return names;
	}

}
