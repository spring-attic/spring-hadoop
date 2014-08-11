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
package org.springframework.yarn.support.statemachine.config.builders;

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer;
import org.springframework.yarn.support.statemachine.config.StateMachineConfig;

public interface StateMachineConfigurer<S, E> extends
		AnnotationConfigurer<StateMachineConfig<S, E>, StateMachineConfigBuilder<S, E>> {

	void configure(StateMachineStateConfigurer<S, E> transitions) throws Exception;

	void configure(StateMachineTransitionConfigurer<S, E> transitions) throws Exception;

}