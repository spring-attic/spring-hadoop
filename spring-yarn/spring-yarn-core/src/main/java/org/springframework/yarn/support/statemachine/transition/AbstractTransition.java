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
package org.springframework.yarn.support.statemachine.transition;

import java.util.Collection;

import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.yarn.support.statemachine.action.Action;
import org.springframework.yarn.support.statemachine.state.State;
import org.springframework.yarn.support.statemachine.trigger.EventTrigger;
import org.springframework.yarn.support.statemachine.trigger.Trigger;

public class AbstractTransition<S, E> implements Transition<S, E> {

	private final State<S,E> source;

	private final State<S,E> target;

	private final Collection<Action> actions;

	private Trigger<S, E> trigger;

	private final TransitionKind kind;

	public AbstractTransition(State<S,E> source, State<S,E> target, Collection<Action> actions, E event, TransitionKind kind) {
		Assert.notNull(source, "Source must be set");
		Assert.notNull(target, "Target must be set");
		Assert.notNull(kind, "Transition type must be set");
		this.source = source;
		this.target = target;
		this.actions = actions;
		if (event != null) {
			this.trigger = new EventTrigger<S, E>(event);
		}
		this.kind = kind;
	}

	@Override
	public State<S,E> getSource() {
		return source;
	}

	@Override
	public State<S,E> getTarget() {
		return target;
	}

	@Override
	public Collection<Action> getActions() {
		return actions;
	}

	@Override
	public Trigger<S, E> getTrigger() {
		return trigger;
	}

	@Override
	public void transit(MessageHeaders headers) {
		if (actions != null) {
			for (Action action : actions) {
				action.execute(headers);
			}
		}
	}

	@Override
	public TransitionKind getKind() {
		return kind;
	}

}
