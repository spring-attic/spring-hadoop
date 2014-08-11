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

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerAdapter;
import org.springframework.yarn.support.statemachine.action.Action;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineTransitionBuilder;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineTransitionConfigurer;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineTransitions;

public class DefaultExternalTransitionConfigurer<S, E>
		extends AnnotationConfigurerAdapter<StateMachineTransitions<S, E>, StateMachineTransitionConfigurer<S, E>, StateMachineTransitionBuilder<S, E>>
		implements ExternalTransitionConfigurer<S, E> {

	private S source;

	private S target;

	private E event;

	private Collection<Action> actions = new ArrayList<Action>();

	@Override
	public void configure(StateMachineTransitionBuilder<S, E> builder) throws Exception {
		builder.add(source, target, event, actions);
	}

	@Override
	public ExternalTransitionConfigurer<S, E> source(S source) {
		this.source = source;
		return this;
	}

	@Override
	public ExternalTransitionConfigurer<S, E> target(S target) {
		this.target = target;
		return this;
	}

	@Override
	public ExternalTransitionConfigurer<S, E> event(E event) {
		this.event = event;
		return this;
	}

	@Override
	public ExternalTransitionConfigurer<S, E> action(Action action) {
		actions.add(action);
		return this;
	}

}
