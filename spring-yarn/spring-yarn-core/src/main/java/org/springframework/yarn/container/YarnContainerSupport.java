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
package org.springframework.yarn.container;

import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.yarn.event.YarnEventPublisher;
import org.springframework.yarn.support.LifecycleObjectSupport;

/**
 * Support object having access to a shared {@link TaskScheduler},
 * {@link TaskExecutor} and {@link YarnEventPublisher}. Currently
 * this is a simple passthrough to {@link LifecycleObjectSupport}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnContainerSupport extends LifecycleObjectSupport {

}
