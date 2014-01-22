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
package org.springframework.yarn.config.annotation;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.configuration.SpringYarnAppmasterConfiguration;
import org.springframework.yarn.config.annotation.configuration.SpringYarnClientConfiguration;
import org.springframework.yarn.config.annotation.configuration.SpringYarnConfiguration;
import org.springframework.yarn.config.annotation.configuration.SpringYarnContainerConfiguration;

/**
 * Spring {@link ImportSelector} choosing appropriate {@link Configuration}
 * based on {@link EnableYarn} annotation.
 *
 * @author Janne Valkealahti
 *
 */
public class SpringYarnConfigurationImportSelector implements ImportSelector {

	private final static Log log = LogFactory.getLog(SpringYarnConfigurationImportSelector.class);

	@Override
	public String[] selectImports(AnnotationMetadata importingClassMetadata) {

		Map<String, Object> attrMap = importingClassMetadata.getAnnotationAttributes(EnableYarn.class.getName());
		AnnotationAttributes enableAttrs = AnnotationAttributes.fromMap(attrMap);
		Enable enable = enableAttrs.getEnum("enable");

		String[] configs = new String[2];

		if (enable == Enable.CLIENT) {
			configs[0] = SpringYarnClientConfiguration.class.getName();
		} else if (enable == Enable.APPMASTER) {
			configs[0] = SpringYarnAppmasterConfiguration.class.getName();
		} else if (enable == Enable.CONTAINER) {
			configs[0] = SpringYarnContainerConfiguration.class.getName();
		} else {
			configs[0] = SpringYarnConfiguration.class.getName();
		}

		configs[1] = ConfiguringBeanFactoryPostProcessorConfiguration.class.getName();

		if (log.isDebugEnabled()) {
			log.debug("Using configs " + configs[0] + "," + configs[1]);
		}

		return configs;
	}

}
