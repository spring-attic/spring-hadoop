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
package org.springframework.yarn.boot.support;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.boot.properties.SpringYarnProperties;

/**
 * Utilities for Spring Yarn Boot.
 *
 * @author Janne Valkealahti
 *
 */
public final class SpringYarnBootUtils {

	private SpringYarnBootUtils(){
		// private no instantiation
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void mergeBootArgumentsIntoMap(String[] args, Map map) {
		if (map == null) {
			throw new IllegalArgumentException("Map must not be null");
		}
		for (String arg : args) {
			// check we have at least --x=y
			if (arg.startsWith("--") && arg.length() > 4) {
				String[] split = arg.substring(2).split("=");
				if (split.length == 2) {
					map.put(split[0], split[1]);
				}
			}
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void mergeHadoopPropertyIntoMap(org.apache.hadoop.conf.Configuration configuration, String configurationKey, String propertyKey, Map map) {
		if (configuration == null) {
			return;
		}
		Assert.state(StringUtils.hasText(configurationKey), "configurationKey must be set");
		Assert.state(StringUtils.hasText(propertyKey), "propertyKey must be set");
		Assert.notNull(map, "map can't be null");
		String value = configuration.get(configurationKey);
		if (value != null) {
			map.put(propertyKey, value);
		}
	}

	public static String[] propertiesToBootArguments(Properties properties) {
		List<String> args = new ArrayList<>();
		for (Entry<Object, Object> entry : properties.entrySet()) {
			args.add("--" + entry.getKey() + "=" + entry.getValue());
		}
		return args.toArray(new String[0]);
	}

	public static void appendAsCommaDelimitedIntoProperties(String key, String[] values, Properties properties) {
		if (!ObjectUtils.isEmpty(values)) {
			String property = properties.getProperty(key);
			if (property == null) {
				property = StringUtils.arrayToCommaDelimitedString(values);
			} else {
				property = property + "," + StringUtils.arrayToCommaDelimitedString(values);
			}
			properties.setProperty(key, property);
		}
	}

	public static void addApplicationListener(SpringApplicationBuilder builder, Properties properties) {
		if (properties != null && !properties.isEmpty()) {
			Map<String, Object> map = new HashMap<>();
			for (String key : properties.stringPropertyNames()) {
				map.put(key, properties.getProperty(key));
			}
			builder.listeners(new YarnBootClientApplicationListener(map));
		}
	}

	public static void addProfiles(SpringApplicationBuilder builder, String[] additionalProfiles) {
		if (!ObjectUtils.isEmpty(additionalProfiles)) {
			builder.profiles(additionalProfiles);
		}
	}

	public static void addSources(SpringApplicationBuilder builder, Class<?>[] sources) {
		if (!ObjectUtils.isEmpty(sources)) {
			builder.sources(sources);
		}
	}

	public static void addConfigFilesContents(SpringApplicationBuilder builder, Map<String, Properties> configFilesContents) {
		if (configFilesContents == null) {
			return;
		}
		Map<String, Object> defaultProperties = new HashMap<>();
		for (Entry<String, Properties> entry : configFilesContents.entrySet()) {
			try {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				entry.getValue().store(out, null);
				defaultProperties.put(
						"spring.yarn.client.localizer.rawFileContents."
								+ SpringYarnBootUtils.escapeConfigKey(entry.getKey()), out.toByteArray());
			} catch (IOException e) {
				// suppress because this should not happen
			}
		}
		builder.properties(defaultProperties);
	}

	public static String escapeConfigKey(String key) {
		return StringUtils.replace(key, ".", "--2E--");
	}

	
	public static String unescapeConfigKey(String key) {
		return StringUtils.replace(key, "--2E--", ".");
	}

	public static String resolveApplicationdir(SpringYarnProperties syp) {
		if (StringUtils.hasText(syp.getApplicationBaseDir()) && StringUtils.hasText(syp.getApplicationVersion())) {
			return (syp.getApplicationBaseDir().endsWith("/") ? syp.getApplicationBaseDir() : syp
					.getApplicationBaseDir() + "/")
					+ syp.getApplicationVersion() + "/";
		} else {
			String dir = syp.getApplicationDir();
			if (StringUtils.hasText(dir) && !dir.endsWith("/")) {
				dir = dir + "/";
			}
			return dir;
		}
	}

}
