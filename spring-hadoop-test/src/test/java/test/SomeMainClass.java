/*
 * Copyright 2011-2013 the original author or authors.
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
package test;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Costin Leau
 */
public class SomeMainClass {

	static {
		System.setProperty("org.springframework.data.jar.init", UUID.randomUUID().toString());
	}

	public static void main(String[] args) throws Exception {
		Configuration cfg = new Configuration();
		cfg.size();

		System.out.println("*** New Config is ***" + dumpConfiguration(cfg).toString());
		System.getProperties().put("org.springframework.data.hadoop.jar.cfg", cfg);
		System.getProperties().put("org.springframework.data.hadoop.jar.args", args);
		System.setProperty("org.springframework.data.jar.exit.pre", "true");
		try {
			System.exit(1);
		} catch (Throwable th) {
			System.getProperties().put("org.springframework.data.jar.exit.exception", th);
		}
	}

	private static String dumpConfiguration(Configuration configuration) {
		StringBuilder sb = new StringBuilder("Config@" + configuration.hashCode());
		sb.append("\n");
		sb.append(configuration.toString());
		sb.append("\n");

		Properties props = new Properties();
		if (configuration != null) {
			for (Map.Entry<String, String> entry : configuration) {
				props.setProperty(entry.getKey(), entry.getValue());
			}
		}

		return sb.append(props.toString()).toString();
	}
}
