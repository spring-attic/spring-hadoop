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
package org.springframework.yarn.integration.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.yarn.support.LifecycleObjectSupport;

/**
 * Extension of {@link LifecycleObjectSupport} adding common
 * {@link ConversionService} for all implementors of this class.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class IntegrationObjectSupport extends LifecycleObjectSupport {

	private static final Log log = LogFactory.getLog(IntegrationObjectSupport.class);

	private volatile ConversionService conversionService;

	/**
	 * Gets the spring conversion service.
	 *
	 * @return the conversion service
	 */
	protected final ConversionService getConversionService() {
		if (this.conversionService == null && getBeanFactory() != null) {
			this.conversionService = IntegrationContextUtils.getConversionService(getBeanFactory());
			if (this.conversionService == null && log.isDebugEnabled()) {
				log.debug("Unable to attempt conversion of Message types. Component '" + this
						+ "' has no explicit ConversionService reference, "
						+ "and there is no 'yarnIntegrationConversionService' bean within the context.");
			}
		}
		return this.conversionService;
	}

	/**
	 * Sets the {@link ConversionService}.
	 *
	 * @param conversionService the conversion service
	 */
	protected void setConversionService(ConversionService conversionService) {
		this.conversionService = conversionService;
	}

}
