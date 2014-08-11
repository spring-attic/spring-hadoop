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
package org.springframework.yarn.support.statemachine.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.yarn.support.LifecycleObjectSupport;
import org.springframework.yarn.support.statemachine.EnumStateMachine;
import org.springframework.yarn.support.statemachine.StateMachine;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineStates;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineStates.StateData;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineTransitions;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineTransitions.TransitionData;
import org.springframework.yarn.support.statemachine.state.EnumState;
import org.springframework.yarn.support.statemachine.state.State;
import org.springframework.yarn.support.statemachine.transition.DefaultExternalTransition;
import org.springframework.yarn.support.statemachine.transition.Transition;

public class EnumStateMachineFactory<S extends Enum<S>, E extends Enum<E>> extends LifecycleObjectSupport implements
		StateMachineFactory<State<S, E>, E> {

	private StateMachineTransitions<S, E> stateMachineTransitions;
	private StateMachineStates<S, E> stateMachineStates;

	public EnumStateMachineFactory(StateMachineTransitions<S, E> stateMachineTransitions,
			StateMachineStates<S, E> stateMachineStates) {
		this.stateMachineTransitions = stateMachineTransitions;
		this.stateMachineStates = stateMachineStates;
	}

	@Override
	public StateMachine<State<S, E>, E> getStateMachine() {
		return stateMachine();
	}

	public StateMachine<State<S, E>, E> stateMachine() {
		Map<S, State<S, E>> stateMap = new HashMap<S, State<S, E>>();
		for (StateData<S, E> stateData : stateMachineStates.getStates()) {
			stateMap.put(stateData.getState(), new EnumState<S, E>(stateData.getState(), stateData.getDeferred()));
		}

		Collection<Transition<S, E>> transitions = new ArrayList<Transition<S, E>>();
		for (TransitionData<S, E> transitionData : stateMachineTransitions.getTransitions()) {
			S source = transitionData.getSource();
			S target = transitionData.getTarget();
			E event = transitionData.getEvent();
			DefaultExternalTransition<S, E> transition = new DefaultExternalTransition<S, E>(stateMap.get(source),
					stateMap.get(target), transitionData.getActions(), event);
			transitions.add(transition);
		}

		EnumStateMachine<S, E> machine = new EnumStateMachine<S, E>(stateMap.values(), transitions, stateMap.get(stateMachineStates
				.getInitialState()));
		machine.afterPropertiesSet();
		if (getBeanFactory() != null) {
			machine.setBeanFactory(getBeanFactory());
		}
		machine.setAutoStartup(isAutoStartup());
		machine.start();
		return machine;
	}

}
