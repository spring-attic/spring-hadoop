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
package org.springframework.yarn.support.statemachine.config.configurers;

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerBuilder;
import org.springframework.yarn.support.statemachine.action.Action;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineTransitionConfigurer;

public interface ExternalTransitionConfigurer<S, E> extends
		AnnotationConfigurerBuilder<StateMachineTransitionConfigurer<S, E>> {

	ExternalTransitionConfigurer<S, E> source(S source);

	ExternalTransitionConfigurer<S, E> target(S source);

	ExternalTransitionConfigurer<S, E> event(E event);

	ExternalTransitionConfigurer<S, E> action(Action action);

}
