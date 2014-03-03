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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.context.MergedContextConfiguration;
import org.springframework.test.context.support.GenericXmlContextLoader;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.test.YarnTestSystemConstants;

/**
 * Extending generic xml based context loader able to
 * manage and inject Yarn mini clusters. This loader is
 * used from {@link YarnDelegatingSmartContextLoader}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnClusterInjectingXmlContextLoader extends GenericXmlContextLoader {

	private final static Log log = LogFactory.getLog(YarnClusterInjectingXmlContextLoader.class);

	@Override
	protected void loadBeanDefinitions(GenericApplicationContext context,
			MergedContextConfiguration mergedConfig) {

		String[] activeProfiles = context.getEnvironment().getActiveProfiles();
		log.info("Active profiles: " + StringUtils.arrayToCommaDelimitedString(activeProfiles));

		// let parent do its magic
		super.loadBeanDefinitions(context, mergedConfig);
		if (!ObjectUtils.containsElement(activeProfiles, YarnTestSystemConstants.PROFILE_ID_NOMINICLUSTER)) {
			YarnClusterInjectUtils.handleClusterInject(context, mergedConfig);
		}
	}

}
