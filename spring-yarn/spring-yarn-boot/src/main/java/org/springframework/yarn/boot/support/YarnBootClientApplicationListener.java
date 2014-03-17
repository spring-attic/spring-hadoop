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

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.CommandLinePropertySource;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.util.Assert;

/**
 * An {@link ApplicationListener} for Spring Boot which is adding a property source
 * relative to the command line arguments.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnBootClientApplicationListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {

	private final static Log log = LogFactory.getLog(YarnBootClientApplicationListener.class);

	private final Map<String, Object> propertySourceMap;

	/**
	 * Instantiates a new yarn boot client application listener.
	 *
	 * @param propertySourceMap the property source map
	 */
	public YarnBootClientApplicationListener(Map<String, Object> propertySourceMap) {
		Assert.notNull(propertySourceMap, "Property source map must be set");
		this.propertySourceMap = propertySourceMap;
	}

	@Override
	public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
		MutablePropertySources propertySources = event.getEnvironment().getPropertySources();
		if (propertySources.contains(CommandLinePropertySource.COMMAND_LINE_PROPERTY_SOURCE_NAME)) {
			if(log.isDebugEnabled()) {
				log.debug("Adding afterCommandLineArgs property source after "
						+ CommandLinePropertySource.COMMAND_LINE_PROPERTY_SOURCE_NAME);
			}
			propertySources.addAfter(CommandLinePropertySource.COMMAND_LINE_PROPERTY_SOURCE_NAME,
					new MapPropertySource("afterCommandLineArgs", propertySourceMap));
		} else {
			if(log.isDebugEnabled()) {
				log.debug("Adding afterCommandLineArgs property source as first");
			}
			propertySources.addFirst(new MapPropertySource("afterCommandLineArgs", propertySourceMap));
		}
	}

}
