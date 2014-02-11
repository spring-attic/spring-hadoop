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

import org.springframework.boot.autoconfigure.batch.JobExecutionEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.yarn.event.YarnEventPublisher;
import org.springframework.yarn.support.LifecycleObjectSupport;

/**
 * Helper class which is listens application events sent by Boot and possibly
 * relays event content back to context as in form understand by Yarn.
 * <p>
 * This class mostly exist because projects not dependent on Boot may
 * want to understand events but can't use classes.
 *
 * @author Janne Valkealahti
 *
 */
public class BootApplicationEventTransformer extends LifecycleObjectSupport implements ApplicationListener<ApplicationEvent> {

	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		if (event instanceof JobExecutionEvent) {
			YarnEventPublisher eventPublisher = getYarnEventPublisher();
			if (eventPublisher != null) {
				eventPublisher.publishEvent(new org.springframework.yarn.batch.event.JobExecutionEvent(this,
						((JobExecutionEvent) event).getJobExecution()));
			}
		}
	}

}
