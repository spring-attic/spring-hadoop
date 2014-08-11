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

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.data.hadoop.config.common.annotation.AbstractConfiguredAnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.ObjectPostProcessor;
import org.springframework.yarn.support.statemachine.action.Action;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineTransitions.TransitionData;
import org.springframework.yarn.support.statemachine.config.configurers.DefaultExternalTransitionConfigurer;
import org.springframework.yarn.support.statemachine.config.configurers.ExternalTransitionConfigurer;

public class StateMachineTransitionBuilder<S, E>
		extends
		AbstractConfiguredAnnotationBuilder<StateMachineTransitions<S, E>, StateMachineTransitionConfigurer<S, E>, StateMachineTransitionBuilder<S, E>>
		implements StateMachineTransitionConfigurer<S, E> {

	private Collection<TransitionData<S, E>> transitionData = new ArrayList<TransitionData<S, E>>();

	public StateMachineTransitionBuilder() {
		super();
	}

	public StateMachineTransitionBuilder(ObjectPostProcessor<Object> objectPostProcessor,
			boolean allowConfigurersOfSameType) {
		super(objectPostProcessor, allowConfigurersOfSameType);
	}

	public StateMachineTransitionBuilder(ObjectPostProcessor<Object> objectPostProcessor) {
		super(objectPostProcessor);
	}

	@Override
	protected StateMachineTransitions<S, E> performBuild() throws Exception {
		StateMachineTransitions<S, E> bean = new StateMachineTransitions<S, E>(transitionData);
		return bean;
	}

	@Override
	public ExternalTransitionConfigurer<S, E> withExternal() throws Exception {
		return apply(new DefaultExternalTransitionConfigurer<S, E>());
	}

	public void add(S source, S target, E event, Collection<Action> actions) {
		transitionData.add(new TransitionData<S, E>(source, target, event, actions));
	}

}
