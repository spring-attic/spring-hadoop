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

import java.util.Collection;

import org.springframework.yarn.support.statemachine.action.Action;

public class StateMachineTransitions<S, E> {

	private Collection<TransitionData<S, E>> transitions;

	public StateMachineTransitions(Collection<TransitionData<S, E>> transitions) {
		this.transitions = transitions;
	}

	public Collection<TransitionData<S, E>> getTransitions() {
		return transitions;
	}

	public static class TransitionData<S, E> {
		S source;
		S target;
		E event;
		Collection<Action> actions;
		public TransitionData(S source, S target, E event, Collection<Action> actions) {
			this.source = source;
			this.target = target;
			this.event = event;
			this.actions = actions;
		}
		public S getSource() {
			return source;
		}
		public S getTarget() {
			return target;
		}
		public E getEvent() {
			return event;
		}
		public Collection<Action> getActions() {
			return actions;
		}
	}

}
